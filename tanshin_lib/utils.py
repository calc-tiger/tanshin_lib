import os
import re
import tempfile
import requests
from pdfminer.high_level import extract_text as pdfminer_extract

def get_text_robust(pdf_path: str) -> str:
    """
    pdfminer.sixを使用してPDFからテキストを抽出します。
    URLが指定された場合はダウンロードして処理します。
    日本語の文字化けに強い抽出を行います。

    Args:
        pdf_path (str): PDFファイルのパスまたはURL。

    Returns:
        str: 抽出されたテキスト。
    """
    temp_file_path = None
    try:
        if pdf_path.startswith(('http://', 'https://')):
            response = requests.get(pdf_path)
            response.raise_for_status()

            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_file:
                temp_file.write(response.content)
                temp_file_path = temp_file.name
            current_pdf_path = temp_file_path
        else:
            current_pdf_path = pdf_path

        # pdfminerでテキスト抽出
        text = pdfminer_extract(current_pdf_path)
        return text

    except Exception as e:
        return f"エラーが発生しました: {e}"
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            os.remove(temp_file_path)

def extract_ticker_from_text(pdf_text: str) -> str:
    """
    PDFのテキスト内容から銘柄コードを抽出します。
    'コード番号' の後に続く4桁の数字、または3桁の数字とアルファベット1文字のパターンを優先して抽出します。

    Args:
        pdf_text (str): PDFから抽出されたテキスト。

    Returns:
        str: 抽出された銘柄コード。見つからない場合はNone。
    """
    # 'コード番号' の後に続く4桁の数字を検索
    match_4_digits = re.search(r'コード番号\s*(\d{4})', pdf_text)
    if match_4_digits:
        code = match_4_digits.group(1)
        if len(code) == 4 and code.isdigit():
            return code

    # 'コード番号' の後に続く3桁の数字とアルファベット1文字を検索 (例: 123A)
    match_3_digits_1_alpha = re.search(r'コード番号\s*(\d{3}[A-Za-zＡ-Ｚａ-ｚ])', pdf_text)
    if match_3_digits_1_alpha:
        code = match_3_digits_1_alpha.group(1)
        if re.fullmatch(r'\d{3}[A-Za-zＡ-Ｚａ-ｚ]', code):
            return code

    return None
