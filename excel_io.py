from __future__ import annotations

import io
from typing import Dict, List

import pandas as pd


def read_excel(file_obj) -> pd.DataFrame:
    return pd.read_excel(file_obj)


def make_rows_from_manual_input(text_area_value: str) -> pd.DataFrame:
    urls = [line.strip() for line in text_area_value.splitlines() if line.strip()]
    return pd.DataFrame({"input_url": urls})


def make_rows_from_excel(df_source: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    # Пояснение: в основной пайплайн передаем только ссылку на раздел каталога.
    return pd.DataFrame({"input_url": df_source[mapping["input_url"]]})


def build_report(records: List[Dict[str, str]]) -> bytes:
    # Пояснение: порядок и названия колонок строго под новое ТЗ.
    report_columns = ["Артикул товара", "Ссылка на сайт", "Ссылка в результатах TinEye"]
    report_df = pd.DataFrame(records, columns=report_columns)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        report_df.to_excel(writer, index=False, sheet_name="Отчет")
    return output.getvalue()
