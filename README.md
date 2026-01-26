# tanshin.lib

日本の適時開示情報（TDnet）などで公開される「決算短信」PDFから、財務データを抽出・解析するPythonライブラリです。

## 特徴

*   **高度なPDF解析**: `pdfplumber` と `pdfminer.six` を組み合わせ、複雑なレイアウトの決算短信から表データを正確に抽出します。
*   **財務データ構造化**: 抽出した表データから、売上高、営業利益などの主要指標を自動的に特定し、構造化データとして出力します。
*   **単位・表記の正規化**: 「百万円」「％」などの単位や、マイナス表記（△）を自動的に処理し、数値として扱いやすい形式に変換します。

## インストール

```bash
pip install git+https://github.com/calc=tiger/tanshin.lib.git
```

## 使い方

### PDFから財務データを抽出する

```python
from tanshin_lib import analyze_pdf_url

# PDFのURLを指定して解析
target_url = "https://www.release.tdnet.info/inbs/140120260114533704.pdf"
financial_data = analyze_pdf_url(target_url)
print(financial_data)
```

## 必要要件

*   Python 3.8+
*   requests
*   pandas
*   pdfminer.six
*   pdfplumber

## ライセンス

MIT License
