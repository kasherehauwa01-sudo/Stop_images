from __future__ import annotations

import datetime as dt
import io
from typing import Dict, List

import pandas as pd


def read_excel(file_obj) -> pd.DataFrame:
    return pd.read_excel(file_obj)


def make_rows_from_manual_input(text_area_value: str) -> pd.DataFrame:
    urls = [line.strip() for line in text_area_value.splitlines() if line.strip()]
    return pd.DataFrame(
        {
            "image_code": ["" for _ in urls],
            "image_url": urls,
            "product_name": ["" for _ in urls],
            "supplier": ["" for _ in urls],
            "manager": ["" for _ in urls],
        }
    )


def make_rows_from_excel(df_source: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    # Пояснение: данные переносятся в отчет без изменений, кроме служебных столбцов поиска.
    return pd.DataFrame(
        {
            "image_code": df_source[mapping["image_code"]],
            "image_url": df_source[mapping["image_url"]],
            "product_name": df_source[mapping["product_name"]],
            "supplier": df_source[mapping["supplier"]],
            "manager": df_source[mapping["manager"]],
        }
    )


def build_report(records: List[Dict[str, str]]) -> bytes:
    report_columns = [
        "Дата проверки",
        "Код изображения",
        "Наименование товара",
        "Поставщик",
        "Менеджер",
        "Ссылка на изображение",
        "Ссылка на сток",
        "Тип источника",
    ]

    report_df = pd.DataFrame(records, columns=report_columns)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        report_df.to_excel(writer, index=False, sheet_name="Отчет")
    return output.getvalue()


def report_row_from_source(row: Dict[str, str], stock_url: str, source_type: str) -> Dict[str, str]:
    return {
        "Дата проверки": dt.datetime.now().strftime("%d.%m.%Y %H:%M"),
        "Код изображения": row.get("image_code", ""),
        "Наименование товара": row.get("product_name", ""),
        "Поставщик": row.get("supplier", ""),
        "Менеджер": row.get("manager", ""),
        "Ссылка на изображение": row.get("image_url", ""),
        "Ссылка на сток": stock_url,
        "Тип источника": source_type,
    }
