import pdfplumber
import pandas as pd
import re
from typing import List, Dict, Any, Optional

def _is_likely_data_or_footnote(text: str) -> bool:
    """
    与えられたテキスト行が、表のタイトルではなく、数値データ行や注釈である可能性が高いかを判断します。
    """
    text_clean = text.replace(' ', '').replace('　', '')

    if not text_clean:
        return True

    if text_clean.lower().startswith('(注)'):
        return True

    unit_header_pattern_clean = re.compile(r'^(?:(?:百万円|千円|円銭|円|銭|株|口|％|%|△|▲|－|＋|[-+])+)+$')
    if unit_header_pattern_clean.fullmatch(text_clean):
        return True

    japanese_chars_pattern = r'[一-龯ぁ-んァ-ヶ]'
    numeric_symbol_chars_pattern = r'[0-9.,%()△▲－＋+-~→～]'

    total_len = len(text_clean)
    descriptive_japanese_count = len(re.findall(japanese_chars_pattern, text_clean))
    numeric_symbol_count = len(re.findall(numeric_symbol_chars_pattern, text_clean))

    unit_keyword_count = 0
    for unit in ["百万円", "千円", "円銭", "株", "口", "％", "%", "円", "銭"]:
        unit_keyword_count += text_clean.count(unit)

    if total_len > 5 and (numeric_symbol_count + unit_keyword_count * 3) / total_len > 0.7:
        if descriptive_japanese_count / total_len < 0.2:
            return True

    if ('年' in text_clean or '期' in text_clean):
        number_like_blocks = re.findall(r'[-－−△▲＋+]?[0-9,]+(?:\.[0-9]+)?%?', text_clean)
        if len(number_like_blocks) >= 2:
            if re.match(r'^[0-9０-９]+\s*[\.\．]', text_clean):
                return False
            else:
                return True

    explanatory_keywords = ["表示は", "対前期増減率", "単位", "算出", "記載", "除く"]
    if any(k in text_clean for k in explanatory_keywords):
        if not any(char.isdigit() for char in text_clean) and descriptive_japanese_count / total_len > 0.5:
            if not re.match(r'^[0-9０-９]+\s*[\.\．]', text_clean) and not text_clean.isupper():
                return True

    return False

def get_table_title(page, table_bbox, max_y_distance=50, max_x_overlap_ratio=0.5) -> Optional[str]:
    """
    テーブルの直上にあるテキストからタイトルを推測します。
    """
    table_x0, table_y0, table_x1, table_y1 = table_bbox
    candidate_titles_raw = []

    all_words = page.extract_words(x_tolerance=3, y_tolerance=3)

    lines_by_y = {}
    line_group_tolerance = 3
    for word in all_words:
        word_y_center = (word["top"] + word["bottom"]) / 2
        found_line_key = None
        for y_key in lines_by_y:
            if abs(word_y_center - y_key) < line_group_tolerance:
                found_line_key = y_key
                break
        if found_line_key is None:
            found_line_key = word_y_center
            lines_by_y[found_line_key] = []
        lines_by_y[found_line_key].append(word)

    sorted_y_keys = sorted(lines_by_y.keys())

    for y_key in sorted_y_keys:
        line_words = sorted(lines_by_y[y_key], key=lambda x: x["x0"])
        line_text = " ".join([w["text"] for w in line_words]).strip()

        if not line_text:
            continue

        line_x0 = min(w["x0"] for w in line_words)
        line_y0 = min(w["top"] for w in line_words)
        line_x1 = max(w["x1"] for w in line_words)
        line_y1 = max(w["bottom"] for w in line_words)

        if line_y1 < table_y0 and (table_y0 - line_y1) <= max_y_distance:
            overlap_x0 = max(table_x0, line_x0)
            overlap_x1 = min(table_x1, line_x1)
            overlap_width = max(0, overlap_x1 - overlap_x0)
            table_width = table_x1 - table_x0
            line_width = line_x1 - line_x0

            if table_width > 0 and line_width > 0:
                overlap_ratio_table = overlap_width / table_width
                if overlap_ratio_table >= max_x_overlap_ratio or \
                   (line_x0 >= table_x0 - (table_width * 0.2) and line_x1 <= table_x1 + (table_width * 0.2)):
                    candidate_titles_raw.append((line_text, table_y0 - line_y1))

    final_candidate_titles = []
    title_keywords = [
        "経営成績", "配当の状況", "財政状態", "キャッシュ・フロー",
        "損益計算書", "損益及び包括利益", "予想", "修正", "概要",
        "連結", "個別", "要約", "報告", "決算", "区分", "集計",
        "会計基準", "重要な会計上の見積り", "事業", "目的"
    ]
    title_prefix_patterns = [
        r'^\s*[\(（][0-9０-９IＶX]+\s*[\)）]',
        r'^\s*[0-9０-９]+\s*[．.]',
        r'^\s*第[0-9０-９]+',
        r'^\s*[ＩＩＩＶX]+\s*[．.]',
    ]

    for line_text, distance in candidate_titles_raw:
        if _is_likely_data_or_footnote(line_text):
            continue

        score = 0
        for keyword in title_keywords:
            if keyword in line_text:
                score += 10
                break

        for pattern in title_prefix_patterns:
            if re.search(pattern, line_text):
                score += 15
                break

        if 5 <= len(line_text) <= 120:
            score += 2
        elif len(line_text) > 0:
            score -= 1

        if line_text.strip() == line_text:
             score += 1

        final_candidate_titles.append((line_text, distance, score))

    if final_candidate_titles:
        final_candidate_titles.sort(key=lambda x: (-x[2], x[1]))
        best_candidate_text, best_candidate_distance, best_candidate_score = final_candidate_titles[0]

        if best_candidate_score >= 10 or (best_candidate_score >= 2 and best_candidate_distance < 15):
             return best_candidate_text
        elif len(final_candidate_titles) == 1 and 5 <= len(best_candidate_text) <= 120:
             return best_candidate_text

    return None

def expand_multiline_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    セル内の改行(\n)を行方向に展開し、全てのデータをフラットな表にします。
    """
    expanded_rows = []

    for i, row in df.iterrows():
        split_cells = [str(x).replace('None', '').split('\n') for x in row]

        merged_split_cells = []
        for cell_parts in split_cells:
            parts = [p.strip() for p in cell_parts]
            parts = [p for p in parts if p]

            if len(parts) < 2:
                merged_split_cells.append(parts)
                continue

            new_parts = []
            skip_next = False

            for j in range(len(parts)):
                if skip_next:
                    skip_next = False
                    continue

                curr_val = parts[j]

                if j < len(parts) - 1:
                    next_val = parts[j+1]
                    curr_clean = curr_val.replace(',', '').replace('△', '-').replace('▲', '-').replace('－', '-')
                    is_curr_num = re.fullmatch(r'^-?\d+(\.\d+)?%?$', curr_clean)
                    match_range_end = re.match(r'^([~～\-－])\s*(.*)$', next_val)

                    is_merged = False
                    if is_curr_num and match_range_end:
                        val_part = match_range_end.group(2)
                        val_part_clean = val_part.replace(',', '').replace('△', '-').replace('▲', '-').replace('－', '-')
                        is_next_num_part = re.fullmatch(r'^-?\d+(\.\d+)?%?$', val_part_clean)

                        if is_next_num_part:
                            combined = f"{curr_val}~{val_part}"
                            new_parts.append(combined)
                            skip_next = True
                            is_merged = True

                    if is_merged:
                        continue

                new_parts.append(curr_val)

            merged_split_cells.append(new_parts)

        max_depth = max([len(c) for c in merged_split_cells]) if merged_split_cells else 0

        if max_depth == 0:
            continue

        for depth in range(max_depth):
            new_row = []
            for cell_parts in merged_split_cells:
                if depth < len(cell_parts):
                    val = cell_parts[depth]
                else:
                    val = ""
                new_row.append(val)
            expanded_rows.append(new_row)

    new_df = pd.DataFrame(expanded_rows)
    new_df = new_df.dropna(axis=1, how='all').dropna(axis=0, how='all').fillna("")

    return new_df

def extract_all_tables_as_df(pdf_path: str, max_pages: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    PDFの全てのページから表を抽出し、Pandas DataFrameのリストとして返します。
    """
    extracted_tables_info = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            if not pdf.pages:
                print(f"PDFファイルにページがありません: {pdf_path}")
                return []

            pages_to_process = pdf.pages[:max_pages] if max_pages is not None else pdf.pages

            table_settings = {
                "vertical_strategy": "lines",
                "horizontal_strategy": "lines",
                "intersection_tolerance": 1,
                "snap_tolerance": 1,
                "join_tolerance": 1,
                "edge_min_length": 3,
            }

            for page_idx, page in enumerate(pages_to_process):
                previous_table_title = ""
                previous_table_y1 = -float('inf')

                tables = page.find_tables(table_settings=table_settings)

                if tables:
                    tables.sort(key=lambda t: t.bbox[1])

                    for table_in_page_idx, table_obj in enumerate(tables):
                        table_bbox = table_obj.bbox
                        _, table_y0, _, table_y1 = table_bbox

                        found_direct_title = get_table_title(page, table_bbox)

                        assigned_title = ""

                        if found_direct_title:
                            assigned_title = found_direct_title
                            previous_table_title = found_direct_title
                        else:
                            if previous_table_title and (table_y0 - previous_table_y1 < 70):
                                assigned_title = previous_table_title

                        df = pd.DataFrame(table_obj.extract()).fillna("")
                        if not df.empty:
                            extracted_tables_info.append({
                                'df': df,
                                'page_number': page_idx + 1,
                                'table_index_on_page': table_in_page_idx + 1,
                                'table_title': assigned_title if assigned_title else ""
                            })

                        previous_table_y1 = table_y1

    except Exception as e:
        print(f"エラーが発生しました: {e}")
        return []

    return extracted_tables_info
