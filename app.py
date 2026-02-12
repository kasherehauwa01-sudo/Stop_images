from __future__ import annotations

import re
from io import BytesIO
from typing import Dict, List, Tuple
from urllib.parse import urlencode, urljoin, urlparse
from zipfile import ZIP_DEFLATED, ZipFile

import streamlit as st
import requests
from bs4 import BeautifulSoup

from excel_io import build_report, make_rows_from_excel, make_rows_from_manual_input, read_excel
from mapping import FIELD_LABELS, REQUIRED_FIELDS_SYNONYMS, auto_map_columns

TIMEOUT_SECONDS = 20
REPORT_CHUNK_SIZE = 50


def append_log(message: str) -> None:
    # Пояснение: собираем лог-строки в session_state, чтобы отображать их прямо в интерфейсе.
    st.session_state.setdefault("ui_logs", [])
    st.session_state["ui_logs"] = (st.session_state["ui_logs"] + [message])[-500:]


def render_logs(placeholder) -> None:
    logs = st.session_state.get("ui_logs", [])
    placeholder.code("\n".join(logs[-200:]) if logs else "Логи пока отсутствуют.", language="text")


def build_tineye_search_url(image_url: str, base_url: str = "https://tineye.com") -> str:
    # Пояснение: формируем URL для сценария "Search by image url".
    return f"{base_url.rstrip('/')}/search?" + urlencode({"url": image_url})


def scrape_product_links_from_section(section_url: str) -> List[str]:
    # Пояснение: из страницы категории собираем ссылки карточек товаров.
    headers = {"User-Agent": "Mozilla/5.0 StopImages/3.0"}
    resp = requests.get(section_url, timeout=TIMEOUT_SECONDS, allow_redirects=True, headers=headers)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    base_host = urlparse(section_url).netloc.lower()

    selectors = [
        "a.product-item-link[href]",
        "a.product-card[href]",
        "a.catalog-item[href]",
        "div.product-item a[href]",
        "article a[href]",
        "a[href]",
    ]

    links: List[str] = []
    seen = set()
    for selector in selectors:
        for a in soup.select(selector):
            href = (a.get("href") or "").strip()
            if not href or href.startswith("#"):
                continue
            full = urljoin(section_url, href)
            parsed = urlparse(full)
            if parsed.scheme not in {"http", "https"}:
                continue
            if parsed.netloc.lower() != base_host:
                continue
            if not any(token in parsed.path.lower() for token in ["/catalog/", "/product/", "/item/"]):
                continue
            if full not in seen:
                seen.add(full)
                links.append(full)
    return links


def _extract_product_image_candidates(soup: BeautifulSoup, product_url: str) -> List[str]:
    # Пояснение: собираем возможные URL изображения из мета-тегов и HTML карточки.
    candidates: List[str] = []

    for selector in ["meta[property='og:image']", "meta[name='twitter:image']", "meta[itemprop='image']"]:
        node = soup.select_one(selector)
        if node and node.get("content"):
            candidates.append(urljoin(product_url, node.get("content", "").strip()))

    for img in soup.select("img"):
        src = (img.get("src") or img.get("data-src") or img.get("data-original") or "").strip()
        if src:
            candidates.append(urljoin(product_url, src))

    unique: List[str] = []
    seen = set()
    for c in candidates:
        if c not in seen:
            seen.add(c)
            unique.append(c)
    return unique


def extract_article_and_image_from_product_page(product_url: str) -> Tuple[str, str]:
    # Пояснение: для отчета берем артикул из карточки и URL изображения для TinEye-ссылки.
    headers = {"User-Agent": "Mozilla/5.0 StopImages/3.0"}
    resp = requests.get(product_url, timeout=TIMEOUT_SECONDS, allow_redirects=True, headers=headers)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    article = ""
    sku_meta = soup.find("meta", attrs={"property": "product:retailer_item_id"}) or soup.find(
        "meta", attrs={"name": "sku"}
    )
    if sku_meta and sku_meta.get("content"):
        article = sku_meta.get("content", "").strip()

    if not article:
        for selector in ["[itemprop='sku']", ".sku", "#sku", "[data-sku]", ".product-sku", ".article", "#article"]:
            node = soup.select_one(selector)
            if node:
                article = (node.get("data-sku") or node.get_text(" ", strip=True) or "").strip()
                if article:
                    break

    image_candidates = _extract_product_image_candidates(soup, product_url)
    if not image_candidates:
        raise ValueError("Не найдено изображение в карточке товара")

    return article, image_candidates[0]


def get_second_level_category_name(section_url: str) -> str:
    # Пояснение: имя файла строим из категории 2-го уровня URL раздела.
    parts = [p for p in urlparse(section_url).path.split("/") if p]
    category = "report"

    if "catalog" in parts:
        idx = parts.index("catalog")
        if len(parts) > idx + 2:
            category = parts[idx + 2]
        elif len(parts) > idx + 1:
            category = parts[idx + 1]
    elif len(parts) >= 2:
        category = parts[1]
    elif parts:
        category = parts[0]

    safe = re.sub(r"[^0-9A-Za-zА-Яа-я_-]+", "_", category).strip("_")
    return safe or "report"


def split_records(records: List[Dict[str, str]], chunk_size: int = REPORT_CHUNK_SIZE) -> List[List[Dict[str, str]]]:
    return [records[i : i + chunk_size] for i in range(0, len(records), chunk_size)]


def build_zip_with_reports(files_payload: List[Tuple[str, bytes]]) -> bytes:
    # Пояснение: если отчетов несколько, отдаем одним ZIP-архивом.
    buf = BytesIO()
    with ZipFile(buf, "w", ZIP_DEFLATED) as zf:
        for filename, payload in files_payload:
            zf.writestr(filename, payload)
    return buf.getvalue()


def process_sections(section_urls: List[str], log_placeholder):
    all_report_files: List[Tuple[str, bytes]] = []
    total_products = 0
    total_errors = 0

    progress = st.progress(0.0)
    total_sections = len(section_urls)

    for section_idx, section_url in enumerate(section_urls, start=1):
        append_log(f"Раздел {section_idx}/{total_sections}: {section_url}")
        render_logs(log_placeholder)

        try:
            product_links = scrape_product_links_from_section(section_url)
            append_log(f"Найдено карточек: {len(product_links)}")
            render_logs(log_placeholder)
        except Exception as exc:
            total_errors += 1
            append_log(f"Ошибка парсинга раздела: {exc}")
            render_logs(log_placeholder)
            progress.progress(section_idx / max(1, total_sections))
            continue

        section_rows: List[Dict[str, str]] = []
        for product_url in product_links:
            try:
                article, image_url = extract_article_and_image_from_product_page(product_url)
                tineye_url = build_tineye_search_url(image_url)
                section_rows.append(
                    {
                        "Артикул": article,
                        "Ссылка на сайт": product_url,
                        "TinEye URL запроса": tineye_url,
                    }
                )
                total_products += 1
                append_log(f"Подготовлен TinEye URL: {tineye_url}")
            except Exception as exc:
                total_errors += 1
                append_log(f"Ошибка карточки {product_url}: {exc}")
            render_logs(log_placeholder)

        chunks = split_records(section_rows, REPORT_CHUNK_SIZE)
        category_name = get_second_level_category_name(section_url)
        for i, chunk_rows in enumerate(chunks, start=1):
            file_suffix = f"_{i}" if len(chunks) > 1 else ""
            filename = f"{category_name}{file_suffix}.xlsx"
            all_report_files.append((filename, build_report(chunk_rows)))

        append_log(
            f"Сформировано файлов по разделу: {len(chunks)} (размер каждого до {REPORT_CHUNK_SIZE} строк)"
        )
        render_logs(log_placeholder)
        progress.progress(section_idx / max(1, total_sections))

    stats = {
        "sections": len(section_urls),
        "products": total_products,
        "errors": total_errors,
        "files": len(all_report_files),
    }
    return all_report_files, stats


def main() -> None:
    st.set_page_config(page_title="TinEye URL-отчеты по карточкам", layout="wide")
    st.title("Генерация TinEye URL по карточкам товаров")

    st.markdown(
        """
- На вход подается ссылка на раздел сайта (или XLS/XLSX со ссылками разделов).
- Приложение обходит карточки товаров и извлекает артикул + URL изображения.
- В отчет записывается: **Артикул**, **Ссылка на сайт**, **TinEye URL запроса**.
- Парсинг результатов TinEye не выполняется.
- Если строк в разделе больше 50, отчет автоматически делится на файлы по 50 строк.
        """
    )

    st.subheader("Логи обработки")
    log_placeholder = st.empty()
    render_logs(log_placeholder)

    input_mode = st.radio("Способ ввода", ["Ручной ввод URL разделов", "Загрузка XLS"], horizontal=True)
    source_df = None
    mapping_confirmed: Dict[str, str] = {}

    if input_mode == "Ручной ввод URL разделов":
        text = st.text_area("Введите ссылки на разделы сайта (по одной на строку)", height=220)
        source_df = make_rows_from_manual_input(text)
        mapping_confirmed = {"input_url": "input_url"}
    else:
        upload = st.file_uploader("Загрузите XLS/XLSX", type=["xls", "xlsx"])
        if upload is not None:
            df_raw = read_excel(upload)
            st.dataframe(df_raw.head(10), use_container_width=True)
            auto_mapping, needs_manual = auto_map_columns(df_raw.columns)
            st.subheader("Сопоставление колонок")
            if not needs_manual:
                mapping_confirmed = {k: v for k, v in auto_mapping.items() if v is not None}
                st.success("Автосопоставление выполнено.")
            else:
                st.warning("Автосопоставление неполное: укажите колонку со ссылкой на раздел вручную.")
                options = [""] + list(df_raw.columns)
                default = auto_mapping.get("input_url") or ""
                selected = st.selectbox(
                    FIELD_LABELS["input_url"],
                    options=options,
                    index=options.index(default) if default in options else 0,
                    key="map_input_url",
                )
                if selected:
                    mapping_confirmed["input_url"] = selected

            if len(mapping_confirmed) == len(REQUIRED_FIELDS_SYNONYMS):
                source_df = make_rows_from_excel(df_raw, mapping_confirmed)

    section_urls: List[str] = []
    if source_df is not None:
        section_urls = [
            str(r.get("input_url", "")).strip()
            for r in source_df.fillna("").to_dict(orient="records")
            if str(r.get("input_url", "")).strip()
        ]

    section_urls = list(dict.fromkeys(section_urls))
    st.info(f"Разделов к обработке: {len(section_urls)}")

    run = st.button("Сформировать отчеты", type="primary", disabled=(len(section_urls) == 0))
    if run:
        st.session_state["ui_logs"] = []
        append_log("Старт обработки")
        render_logs(log_placeholder)

        files_payload, stats = process_sections(section_urls, log_placeholder)

        st.success(
            f"Готово. Разделов: {stats['sections']} | Карточек: {stats['products']} | "
            f"Ошибок: {stats['errors']} | Файлов XLSX: {stats['files']}"
        )

        if not files_payload:
            st.info("Нет данных для формирования отчетов.")
            return

        if len(files_payload) == 1:
            filename, payload = files_payload[0]
            st.download_button(
                label=f"Скачать {filename}",
                data=payload,
                file_name=filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        else:
            zip_payload = build_zip_with_reports(files_payload)
            st.download_button(
                label="Скачать все отчеты ZIP",
                data=zip_payload,
                file_name="tineye_reports.zip",
                mime="application/zip",
            )
            with st.expander("Список сформированных файлов", expanded=False):
                for filename, _ in files_payload:
                    st.write(filename)


if __name__ == "__main__":
    main()
