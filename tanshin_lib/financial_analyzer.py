import pandas as pd
import re
import requests
import tempfile
import os
from typing import List, Dict, Any, Optional

from .utils import get_text_robust, extract_ticker_from_text
from .pdf_parser import extract_all_tables_as_df, expand_multiline_table

def parse_financial_table(df):
    """
    展開された財務諸表DataFrameから、検出されたすべての数値指標とその増減率を抽出します。
    ヘッダーの行数を動的に判定し、複数行にまたがるカラム名を結合します。
    """
    extracted_records = []
    if df.empty:
        return []

    # --- Step 1: データ開始行の検出 (Detect Data Start Row) ---
    first_data_row_idx = -1

    for i in range(len(df)):
        row_label = str(df.iloc[i, 0]).strip()

        if re.search(r'(?:19|20)\d{2}[^0-9]*[年\./]', row_label) or \
           '通期' in row_label or \
           '四半期' in row_label or \
           '年度' in row_label:
            first_data_row_idx = i
            break

        if len(df.columns) > 1:
            numeric_count = 0
            data_cols_count = len(df.columns) - 1
            for val in df.iloc[i, 1:]:
                s_val = str(val).strip().replace(',', '').replace('△', '-').replace('▲', '-').replace('－', '-')
                if re.fullmatch(r'[-]?\d+(\.\d+)?%?', s_val) or s_val in ['-', '−', '―']:
                    numeric_count += 1
            if numeric_count / data_cols_count > 0.5:
                first_data_row_idx = i
                break

    if first_data_row_idx == -1:
        return []
    if first_data_row_idx == 0:
        first_data_row_idx = 1

    # --- Step 2: カラムヘッダーの構築 ---
    final_column_headers = ['period']

    for c_idx in range(1, len(df.columns)):
        header_parts = []
        for r in range(first_data_row_idx):
            val = str(df.iloc[r, c_idx]).strip()
            if val and val.lower() != 'nan' and val != 'None':
                header_parts.append(val)

        full_header_text = "".join(header_parts)
        
        # --- ユニットサフィックスの初期判定 ---
        unit_suffix = ""
        if '百万円' in full_header_text:
            unit_suffix = "_百万円"
            full_header_text = full_header_text.replace('(百万円)', '').replace('（百万円）', '').replace('百万円', '')
        elif '円銭' in full_header_text:
            unit_suffix = "_円銭"
            full_header_text = full_header_text.replace('(円銭)', '').replace('（円銭）', '').replace('円銭', '')

        # --- サフィックスのみ（増減率など）かどうかの判定 ---
        is_suffix_only = False
        if re.fullmatch(r'[\(（]*[％%][\)）]*', full_header_text.strip()) or \
           any(k in full_header_text for k in ['増減率', '前年比', '対前年', '同増減', '対前期']): 
            is_suffix_only = True

        # クリーンなヘッダー名作成
        clean_header = full_header_text.replace(' ', '').replace('　', '').replace('\n', '')

        # --- ヘッダー名の決定ロジック ---
        # ヘッダーが空、またはサフィックスのみ（%など）の場合は、前の列名を引き継ぐ
        if not clean_header or is_suffix_only:
            if c_idx > 1:
                prev_header_full = final_column_headers[-1]
                # 前の列名から既存のサフィックスを除去してベース名を取得
                prev_base = re.sub(r'_(百万円|円銭|増減率)$', '', prev_header_full)
                # 重複回避の _1 等も除去したほうが安全だが、通常はsuffixの後につくのでここではbaseのみ
                
                clean_header = prev_base
                
                # 空欄またはサフィックスのみで引き継いだ場合、かつ通貨単位などがついていなければ「増減率」とみなす
                if '増減率' not in unit_suffix:
                    # すでに % があって unit_suffix が _増減率 になっている場合もあるのでチェック
                    # もし unit_suffix が空なら、強制的に増減率扱いにする
                     if unit_suffix == "":
                         unit_suffix = "_増減率"
            else:
                clean_header = f"col_{c_idx}"
        
        # 明示的に増減率が含まれている場合の補正
        if ('増減率' in full_header_text or '％' in full_header_text or '%' in full_header_text) and '増減率' not in unit_suffix:
             unit_suffix = "_増減率"

        final_name = clean_header + unit_suffix
        original_name = final_name
        counter = 1
        while final_name in final_column_headers:
            final_name = f"{original_name}_{counter}"
            counter += 1
        final_column_headers.append(final_name)

    # --- Step 3: データの抽出 ---
    queued_labels = []
    persistent_pending_range_starts = {header: None for header in final_column_headers[1:]}

    for i in range(first_data_row_idx, len(df)):
        row = df.iloc[i]
        row_label_text = str(row[0]).strip()

        is_period_label = False
        if '年' in row_label_text or '期' in row_label_text or '通期' in row_label_text or '年度' in row_label_text:
            is_period_label = True

        has_numeric_data = False
        current_data_cells = [str(c).strip() for c in row]
        if any(re.search(r'[0-9]', cell_val) or cell_val in ['-', '－', '―', '−'] for cell_val in current_data_cells[1:]):
            has_numeric_data = True

        target_period = None
        if has_numeric_data:
            if queued_labels:
                target_period = queued_labels.pop(0)
                if is_period_label:
                    queued_labels.append(row_label_text)
            else:
                if is_period_label:
                    target_period = row_label_text
        elif is_period_label:
            queued_labels.append(row_label_text)

        current_row_parsed_data = {}
        row_has_meaningful_data = False

        for c_idx in range(1, len(current_data_cells)):
            if c_idx < len(final_column_headers):
                header = final_column_headers[c_idx]
                cell_value_raw = current_data_cells[c_idx]

                normalized_val = cell_value_raw.replace('△', '-').replace('▲', '-').replace('－', '-').replace('＋', '').replace('+', '')
                normalized_val = normalized_val.replace('～', '~')

                if not normalized_val or normalized_val in ['-', '―', '−', 'nan', 'None']:
                    current_row_parsed_data[header] = None
                    persistent_pending_range_starts[header] = None
                    continue

                full_range_match = re.search(r'([-]?[0-9,.]+)\s*[~-]\s*([-]?[0-9,.]+)', normalized_val)
                if full_range_match:
                    current_row_parsed_data[header] = f"{full_range_match.group(1)}~{full_range_match.group(2)}"
                    persistent_pending_range_starts[header] = None
                    row_has_meaningful_data = True
                    continue

                partial_range_start_match = re.fullmatch(r'([-]?[0-9,.]+)\s*[~-]', normalized_val)
                if partial_range_start_match:
                    persistent_pending_range_starts[header] = partial_range_start_match.group(1)
                    current_row_parsed_data[header] = normalized_val
                    row_has_meaningful_data = True
                    continue

                plain_number_match = re.fullmatch(r'([-]?[0-9,.]+)', normalized_val)
                if plain_number_match and persistent_pending_range_starts[header] is not None:
                    full_range_str = f"{persistent_pending_range_starts[header]}~{plain_number_match.group(1)}"
                    current_row_parsed_data[header] = full_range_str
                    persistent_pending_range_starts[header] = None
                    row_has_meaningful_data = True
                    continue

                partial_range_end_match = re.fullmatch(r'[~-]\s*([-]?[0-9,.]+)', normalized_val)
                if partial_range_end_match and persistent_pending_range_starts[header] is not None:
                    full_range_str = f"{persistent_pending_range_starts[header]}~{partial_range_end_match.group(1)}"
                    current_row_parsed_data[header] = full_range_str
                    persistent_pending_range_starts[header] = None
                    row_has_meaningful_data = True
                    continue
                elif persistent_pending_range_starts[header] is not None:
                    persistent_pending_range_starts[header] = None

                try:
                    if re.fullmatch(r'^-?[0-9,.]+$', normalized_val):
                        val_float = float(normalized_val.replace(',', ''))
                        current_row_parsed_data[header] = val_float
                        row_has_meaningful_data = True
                        continue
                except ValueError:
                    pass

                if re.fullmatch(r'^(百万円|千円|円銭|％|%|円|銭)$', normalized_val):
                    current_row_parsed_data[header] = None
                else:
                    current_row_parsed_data[header] = cell_value_raw
                row_has_meaningful_data = True

        if target_period:
            record = {'period': target_period}
            record.update(current_row_parsed_data)
            if any(v is not None for k, v in current_row_parsed_data.items() if k != 'period'):
                 extracted_records.append(record)

        elif row_has_meaningful_data and extracted_records:
            last_record = extracted_records[-1]
            merged_any = False
            for header, current_val in current_row_parsed_data.items():
                if header == 'period': continue
                prev_val = last_record.get(header)
                if prev_val is None and current_val is not None:
                    last_record[header] = current_val
                    merged_any = True

    return extracted_records


def _format_value_for_display(value):
    def _format_num_for_delta_display(num_str):
        num_str = str(num_str).replace(' ', '').replace('　', '').replace(',', '')
        if num_str.startswith('-'):
            return '△' + num_str[1:]
        return num_str

    if pd.isna(value):
        return None

    if isinstance(value, str):
        normalized_value_str = value.replace('△', '-').replace('▲', '-').replace('－', '-')
        normalized_value_str = normalized_value_str.replace('～', '~')

        range_match = re.search(r'([-]?[0-9,.]+)\s*[~-]\s*([-]?[0-9,.]+)', normalized_value_str)

        if range_match:
            part1_raw = range_match.group(1).strip()
            part2_raw = range_match.group(2).strip()
            return f"{_format_num_for_delta_display(part1_raw)}~{_format_num_for_delta_display(part2_raw)}"

        if normalized_value_str.startswith('-') and re.fullmatch(r'-?[0-9,.]+', normalized_value_str):
            return '△' + normalized_value_str[1:]
        return value

    elif isinstance(value, (int, float)):
        if value < 0:
            if value == 0.0:
                return '0.0'
            return '△' + str(abs(value))
        return str(value)

    return value

def _format_value_for_display_with_unit(value, metric_name):
    formatted_value = _format_value_for_display(value)
    if formatted_value is None:
        return ""

    formatted_value_str = str(formatted_value)
    cleaned_val = formatted_value_str.replace('%', '').replace('％', '').replace('百万円', '').replace('円銭', '').replace('円', '').replace('銭', '')

    range_match = re.search(r'(.+)~(.+)', cleaned_val)

    unit = ""
    if '増減率' in metric_name or '%' in metric_name or '％' in metric_name:
        unit = "%"
    elif '百万円' in metric_name:
        unit = "百万円"
    elif '円銭' in metric_name:
        unit = "円銭"

    if range_match:
        part1 = range_match.group(1)
        part2 = range_match.group(2)
        return f"{part1}{unit}~{part2}{unit}"
    else:
        return f"{cleaned_val}{unit}"

def analyze_pdf_url(pdf_target: str, ticker: Optional[str] = None, verbose: bool = True) -> pd.DataFrame:
    """
    指定されたパス(URLまたはローカルファイル)のPDFを解析し、
    「経営成績」および「業績予想」に関連するデータのみを抽出して返します。
    """
    if verbose:
        print(f"Target: {pdf_target}")

    temp_file_path = None
    try:
        if pdf_target.startswith(('http://', 'https://')):
            if verbose: print("Downloading PDF...")
            response = requests.get(pdf_target)
            response.raise_for_status()
            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_file:
                temp_file.write(response.content)
                temp_file_path = temp_file.name
            current_pdf_path = temp_file_path
        else:
            current_pdf_path = pdf_target
            if not os.path.exists(current_pdf_path):
                if verbose: print(f"File not found: {current_pdf_path}")
                return pd.DataFrame()

        if ticker is None:
            full_text = get_text_robust(current_pdf_path)
            auto_ticker = extract_ticker_from_text(full_text)
            ticker = auto_ticker if auto_ticker else 'UNKNOWN'
            if verbose: print(f"Ticker detected: {ticker}")

        extracted_tables_info = extract_all_tables_as_df(current_pdf_path, max_pages=2)

        if not extracted_tables_info:
            if verbose: print("表が見つかりませんでした。")
            return pd.DataFrame()

        parsed_records = []
        for item in extracted_tables_info:
            df_table = item['df']
            table_title = item['table_title']

            if verbose:
                print(f"  Processing Table: {table_title}")

            expanded_df = expand_multiline_table(df_table)
            if expanded_df.empty:
                continue

            records = parse_financial_table(expanded_df)

            for r in records:
                r['ticker'] = ticker
                r['pdf_path'] = pdf_target
                r['table_title'] = table_title
                r['page_number'] = item['page_number']

            parsed_records.extend(records)

        if parsed_records:
            result_df = pd.DataFrame(parsed_records)
            target_categories = ["経営成績","業績", "業績予想"]
            filtered_df = pd.DataFrame()
            for cat in target_categories:
                subset = result_df[result_df['table_title'].str.contains(cat, na=False)]
                if not subset.empty:
                        filtered_df = pd.concat([filtered_df, subset])

            if not filtered_df.empty:
                filtered_df = filtered_df.drop_duplicates()

            if verbose:
                if not filtered_df.empty:
                    print(f"\n--- Extracted Financial Data Summary ({ticker}) ---")
                    metadata_cols = ['ticker', 'pdf_path', 'page_number', 'table_title']
                    grouped_for_display = filtered_df.groupby('table_title')

                    for title, group_df in grouped_for_display:
                        print(f"\nテーブルタイトル: {title}")

                        financial_cols = [col for col in group_df.columns if col not in metadata_cols and col != 'period']
                        if not financial_cols:
                            continue

                        df_for_display_prep = group_df[['period'] + financial_cols]
                        df_to_display_aggregated = df_for_display_prep.groupby('period').first()

                        displayed_df = df_to_display_aggregated.T
                        displayed_df = displayed_df.dropna(axis=1, how='all')
                        displayed_df = displayed_df.dropna(axis=0, how='all')
                        displayed_df.index.name = None

                        formatted_df = pd.DataFrame(index=displayed_df.index, columns=displayed_df.columns)
                        for idx, row in displayed_df.iterrows():
                            metric_name = idx
                            formatted_df.loc[idx] = row.apply(lambda x: _format_value_for_display_with_unit(x, metric_name))

                        display(formatted_df.fillna(""))
                        print("-"*40)
                else:
                    print("  (No relevant financial tables found matching filters)")

            return filtered_df
        else:
            if verbose: print("有効な財務データが抽出できませんでした。")
            return pd.DataFrame()

    except Exception as e:
        if verbose: print(f"Error processing {pdf_target}: {e}")
        return pd.DataFrame()
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            os.remove(temp_file_path)
