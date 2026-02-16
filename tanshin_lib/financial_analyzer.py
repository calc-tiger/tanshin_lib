import pandas as pd
import re
import requests
import tempfile
import os
from typing import List, Dict, Any, Optional

from .utils import get_text_robust, extract_ticker_from_text
from .pdf_parser import extract_all_tables_as_df, expand_multiline_table

def parse_financial_table(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    展開された財務諸表DataFrameから、検出されたすべての数値指標とその増減率を抽出します。
    """
    extracted_records = []
    if df.empty:
        return []

    final_column_headers = ['period']

    col_base_metric_names = [''] * len(df.columns)

    for c_idx in range(1, len(df.columns)):
        header_parts = []
        # データ開始行より前の行をすべてループ
        for r in range(first_data_row_idx):
            val = str(df.iloc[r, c_idx]).strip()
            if val and val.lower() != 'nan' and val != 'None':
                header_parts.append(val)

        combined_metric_part ="".join(header_parts)

        is_suffix_only = False
        
        if any(k in combined_metric_part for k in ['増減率', '前年比', '対前年', '同増減', '対前年同四半期', '対前年同期']):
             is_suffix_only = True
        elif re.fullmatch(r'[\(（]*[％%][\)）]*', combined_metric_part):
             is_suffix_only = True

        if combined_metric_part and not is_suffix_only:
            col_base_metric_names[c_idx] = combined_metric_part
        elif c_idx > 1 and col_base_metric_names[c_idx-1]:
             col_base_metric_names[c_idx] = col_base_metric_names[c_idx-1]
             if '増減率' not in col_base_metric_names[c_idx]:
                  if is_suffix_only or not combined_metric_part:
                      col_base_metric_names[c_idx] += '_増減率'

    for c_idx in range(1, len(df.columns)):
        header_base = col_base_metric_names[c_idx]
        unit_type_suffix = ""

        if 2 < len(df) and c_idx < len(df.iloc[2]):
            unit_cell_content = str(df.iloc[2, c_idx]).strip()
            if '百万円' in unit_cell_content:
                unit_type_suffix = '_百万円'
            elif ('％' in unit_cell_content or '%' in unit_cell_content) and '増減率' not in header_base:
                unit_type_suffix = '_増減率'
            elif '円銭' in unit_cell_content:
                unit_type_suffix = '_円銭'

        constructed_header = (header_base + unit_type_suffix).strip('_')
        if not constructed_header:
            constructed_header = f"col_{c_idx}"

        original_header = constructed_header
        counter = 1
        while constructed_header in final_column_headers:
            constructed_header = f"{original_header}_{counter}"
            counter += 1
        final_column_headers.append(constructed_header)

    queued_labels = []
    persistent_pending_range_starts = {header: None for header in final_column_headers[1:]}

    for i, row in df.iterrows():
        row_label_text = str(row[0]).strip()

        is_period_label = False
        if '年' in row_label_text or '期' in row_label_text or '通期' in row_label_text:
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

                if not normalized_val or normalized_val in ['-', '―', '−']:
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

                if normalized_val.endswith('%') or normalized_val.endswith('％'):
                    normalized_val = normalized_val.rstrip('％%')

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
                elif isinstance(prev_val, str) and prev_val.endswith('~') and \
                     isinstance(current_val, str) and '~' in current_val and not current_val.endswith('~'):
                    last_record[header] = current_val
                    merged_any = True
            if merged_any:
                pass
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
