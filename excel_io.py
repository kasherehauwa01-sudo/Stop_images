from __future__ import annotations

import io
from typing import Dict, List

import pandas as pd
from openpyxl.styles import Font


def read_excel(file_obj) -> pd.DataFrame:
    return pd.read_excel(file_obj)


def make_rows_from_manual_input(text_area_value: str) -> pd.DataFrame:
    urls = [line.strip() for line in text_area_value.splitlines() if line.strip()]
    return pd.DataFrame({"input_url": urls})


def make_rows_from_excel(df_source: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    # Пояснение: в основной пайплайн передаем только ссылку на раздел каталога.
    return pd.DataFrame({"input_url": df_source[mapping["input_url"]]})


def build_report(records: List[Dict[str, str]]) -> bytes:
    # Пояснение: порядок колонок в XLSX строго по обновленному ТЗ.
    report_columns = ["Артикул", "Ссылка на сайт", "TinEye URL запроса"]
    report_df = pd.DataFrame(records, columns=report_columns)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        report_df.to_excel(writer, index=False, sheet_name="Отчет")
        ws = writer.book["Отчет"]

        # Пояснение: делаем ссылки активными и задаем анкор "Результат" для TinEye URL.
        link_font = Font(color="0563C1", underline="single")

        max_site_len = len("Ссылка на сайт")
        for row_idx in range(2, len(report_df) + 2):
            site_cell = ws.cell(row=row_idx, column=2)
            site_url = str(site_cell.value or "").strip()
            if site_url:
                site_cell.hyperlink = site_url
                site_cell.style = "Hyperlink"
                max_site_len = max(max_site_len, len(site_url))

            tineye_cell = ws.cell(row=row_idx, column=3)
            tineye_url = str(tineye_cell.value or "").strip()
            if tineye_url:
                tineye_cell.hyperlink = tineye_url
                tineye_cell.value = "Результат"
                tineye_cell.font = link_font

        # Пояснение: подгоняем ширину колонки "Ссылка на сайт" под самую длинную ссылку.
        ws.column_dimensions["B"].width = min(max_site_len + 2, 140)
        ws.column_dimensions["C"].width = max(len("TinEye URL запроса"), len("Результат")) + 4

    return output.getvalue()
