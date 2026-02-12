from __future__ import annotations

import json
import re
from io import BytesIO
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode, urljoin, urlparse
from zipfile import ZIP_DEFLATED, ZipFile

import requests
import streamlit as st
from bs4 import BeautifulSoup

from excel_io import build_report, make_rows_from_excel, make_rows_from_manual_input, read_excel
from mapping import FIELD_LABELS, REQUIRED_FIELDS_SYNONYMS, auto_map_columns

TIMEOUT_SECONDS = 20
REPORT_CHUNK_SIZE = 50
USER_AGENT = "Mozilla/5.0 StopImages/3.1"


# Пояснение: признаки технических изображений, которые не нужно использовать как главное фото товара.
TECH_IMAGE_HINTS = [
    "icon",
    "sprite",
    "logo",
    "placeholder",
    "favicon",
    "badge",
    "payment",
    "delivery",
    "thumb",
    "thumbnail",
    "loading",
]


# Пояснение: фразы для определения наличия товара.
IN_STOCK_HINTS = ["в наличии", "есть в наличии", "in stock", "добавить в корзину", "купить"]
OUT_OF_STOCK_HINTS = ["нет в наличии", "под заказ", "ожидается", "out of stock", "нет на складе"]


def append_log(message: str) -> None:
    # Пояснение: логи храним в session_state, чтобы пользователь видел ход обработки.
    st.session_state.setdefault("ui_logs", [])
    st.session_state["ui_logs"] = (st.session_state["ui_logs"] + [message])[-700:]


def render_logs(placeholder=None) -> None:
    logs = st.session_state.get("ui_logs", [])
    if placeholder is None:
        return
    placeholder.code("\n".join(logs[-250:]) if logs else "Логи пока отсутствуют.", language="text")


def build_tineye_search_url(image_url: str, base_url: str = "https://tineye.com") -> str:
    # Пояснение: формируем URL для сценария "Search by image url".
    return f"{base_url.rstrip('/')}/search?" + urlencode({"url": image_url})


def scrape_product_links_from_section(section_url: str) -> List[str]:
    # Пояснение: собираем ссылки карточек товаров из раздела.
    headers = {"User-Agent": USER_AGENT}
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


def is_probably_technical_image(url: str, css_classes: str = "", alt_text: str = "") -> bool:
    # Пояснение: фильтрация технических картинок, иконок и служебной графики.
    sample = f"{url} {css_classes} {alt_text}".lower()
    return any(hint in sample for hint in TECH_IMAGE_HINTS)


def extract_main_product_image(soup: BeautifulSoup, product_url: str) -> Optional[str]:
    # Пояснение: берем именно главное изображение карточки, а не все картинки страницы.
    priority_selectors = [
        "meta[property='og:image']",
        "meta[name='twitter:image']",
        "meta[itemprop='image']",
        ".product-detail img[itemprop='image']",
        ".product-main-image img",
        ".product-gallery__main img",
        ".product-gallery img",
        ".product-card__image img",
    ]

    for selector in priority_selectors:
        for node in soup.select(selector):
            raw = (node.get("content") or node.get("src") or node.get("data-src") or "").strip()
            if not raw:
                continue
            full = urljoin(product_url, raw)
            if is_probably_technical_image(full, " ".join(node.get("class", [])), node.get("alt", "")):
                continue
            return full

    # Пояснение: fallback — первый нетехнический img, если явного главного селектора нет.
    for img in soup.select("img"):
        raw = (img.get("src") or img.get("data-src") or img.get("data-original") or "").strip()
        if not raw:
            continue
        full = urljoin(product_url, raw)
        if is_probably_technical_image(full, " ".join(img.get("class", [])), img.get("alt", "")):
            continue
        return full

    return None


def extract_article(soup: BeautifulSoup) -> str:
    # Пояснение: артикул берем из мета-тегов или типовых блоков карточки.
    sku_meta = soup.find("meta", attrs={"property": "product:retailer_item_id"}) or soup.find(
        "meta", attrs={"name": "sku"}
    )
    if sku_meta and sku_meta.get("content"):
        return sku_meta.get("content", "").strip()

    for selector in ["[itemprop='sku']", ".sku", "#sku", "[data-sku]", ".product-sku", ".article", "#article"]:
        node = soup.select_one(selector)
        if node:
            val = (node.get("data-sku") or node.get_text(" ", strip=True) or "").strip()
            if val:
                return val
    return ""


def _availability_from_json_ld(soup: BeautifulSoup) -> Optional[bool]:
    # Пояснение: сначала пытаемся получить статус наличия из структурированных данных JSON-LD.
    for script in soup.select("script[type='application/ld+json']"):
        text = (script.string or script.get_text() or "").strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except Exception:
            continue

        stack = [payload]
        while stack:
            item = stack.pop()
            if isinstance(item, dict):
                availability = str(item.get("availability", "")).lower()
                if "instock" in availability:
                    return True
                if "outofstock" in availability:
                    return False
                for v in item.values():
                    stack.append(v)
            elif isinstance(item, list):
                stack.extend(item)
    return None


def is_product_in_stock(soup: BeautifulSoup) -> bool:
    # Пояснение: проверяем наличие товара; только товары "в наличии" идут в обработку.
    availability_node = soup.select_one("link[itemprop='availability'], meta[itemprop='availability']")
    if availability_node and availability_node.get("href"):
        href = availability_node.get("href", "").lower()
        if "instock" in href:
            return True
        if "outofstock" in href:
            return False

    json_ld_state = _availability_from_json_ld(soup)
    if json_ld_state is not None:
        return json_ld_state

    text = soup.get_text(" ", strip=True).lower()
    if any(neg in text for neg in OUT_OF_STOCK_HINTS):
        return False
    if any(pos in text for pos in IN_STOCK_HINTS):
        return True

    buy_btn = soup.select_one("button.add-to-cart, button.buy, .btn-buy, .to-cart")
    if buy_btn and not buy_btn.has_attr("disabled"):
        return True

    return False


def extract_product_data(product_url: str) -> Tuple[str, str, bool]:
    # Пояснение: из карточки получаем артикул, главное изображение и статус наличия.
    headers = {"User-Agent": USER_AGENT}
    resp = requests.get(product_url, timeout=TIMEOUT_SECONDS, allow_redirects=True, headers=headers)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    article = extract_article(soup)
    in_stock = is_product_in_stock(soup)
    image_url = extract_main_product_image(soup, product_url)

    if not image_url:
        raise ValueError("Не найдено главное изображение товара")

    return article, image_url, in_stock


def get_second_level_category_name(section_url: str) -> str:
    # Пояснение: имя файла формируется по категории 2-го уровня URL раздела.
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
    # Пояснение: при нескольких отчетах отдаем один ZIP.
    buf = BytesIO()
    with ZipFile(buf, "w", ZIP_DEFLATED) as zf:
        for filename, payload in files_payload:
            zf.writestr(filename, payload)
    return buf.getvalue()


def process_sections(section_urls: List[str], log_placeholder):
    all_report_files: List[Tuple[str, bytes]] = []
    total_checked = 0
    total_in_stock = 0
    total_skipped_not_in_stock = 0
    total_errors = 0

    progress = st.progress(0.0)
    total_sections = len(section_urls)

    for section_idx, section_url in enumerate(section_urls, start=1):
        append_log(f"Раздел {section_idx}/{total_sections}: {section_url}")
        render_logs(log_placeholder)

        try:
            product_links = scrape_product_links_from_section(section_url)
            append_log(f"Найдено карточек в разделе: {len(product_links)}")
            render_logs(log_placeholder)
        except Exception as exc:
            total_errors += 1
            append_log(f"Ошибка парсинга раздела: {exc}")
            render_logs(log_placeholder)
            progress.progress(section_idx / max(1, total_sections))
            continue

        section_rows: List[Dict[str, str]] = []
        for product_url in product_links:
            if st.session_state.get("stop_requested", False):
                append_log("Получена команда СТОП. Останавливаем обработку после текущего шага.")
                render_logs(log_placeholder)
                break

            total_checked += 1
            try:
                article, image_url, in_stock = extract_product_data(product_url)

                if not in_stock:
                    total_skipped_not_in_stock += 1
                    append_log(f"Пропущено (нет в наличии): {product_url}")
                    render_logs(log_placeholder)
                    continue

                total_in_stock += 1
                tineye_url = build_tineye_search_url(image_url)
                section_rows.append(
                    {
                        "Артикул": article,
                        "Ссылка на сайт": product_url,
                        "TinEye URL запроса": tineye_url,
                    }
                )
                append_log(f"OK (в наличии): {product_url} | Главное изображение: {image_url}")
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
            f"Файлов по разделу: {len(chunks)} | учтены только товары в наличии"
        )
        render_logs(log_placeholder)
        progress.progress(section_idx / max(1, total_sections))

        if st.session_state.get("stop_requested", False):
            break

    stats = {
        "sections": len(section_urls),
        "checked": total_checked,
        "in_stock": total_in_stock,
        "skipped_not_in_stock": total_skipped_not_in_stock,
        "errors": total_errors,
        "files": len(all_report_files),
    }
    return all_report_files, stats


def main() -> None:
    st.set_page_config(page_title="TinEye URL-отчеты по товарам", layout="wide")
    st.title("Генерация TinEye URL по товарам в наличии")

    st.markdown(
        """
- На вход подается ссылка на раздел сайта (или XLS/XLSX со ссылками разделов).
- Приложение обходит карточки товаров в разделе.
- Берет **только товары в наличии**.
- Из карточки берет **главное изображение товара** (иконки/технические картинки фильтруются).
- В отчет записывается: **Артикул**, **Ссылка на сайт**, **TinEye URL запроса**.
- Если строк больше 50, отчет делится на файлы по 50 строк.
        """
    )

    st.session_state.setdefault("stop_requested", False)

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

    # Пояснение: кнопки управления размещены под полем ввода URL разделов.
    ctrl_col1, ctrl_col2 = st.columns([1, 1])
    with ctrl_col1:
        run = st.button("Сформировать отчеты", type="primary")
    with ctrl_col2:
        stop_clicked = st.button("Стоп", type="secondary")
        if stop_clicked:
            st.session_state["stop_requested"] = True

    if st.session_state.get("stop_requested", False):
        st.warning("Запрошена остановка обработки. Для нового запуска нажмите «Сформировать отчеты».")

    section_urls: List[str] = []
    if source_df is not None:
        section_urls = [
            str(r.get("input_url", "")).strip()
            for r in source_df.fillna("").to_dict(orient="records")
            if str(r.get("input_url", "")).strip()
        ]

    section_urls = list(dict.fromkeys(section_urls))
    st.info(f"Разделов к обработке: {len(section_urls)}")

    files_payload: List[Tuple[str, bytes]] = []
    stats = None

    log_placeholder = None

    if run:
        st.session_state["ui_logs"] = []
        st.session_state["stop_requested"] = False
        append_log("Старт обработки")

        if not section_urls:
            st.warning("Нет ссылок разделов для обработки.")
            return

        files_payload, stats = process_sections(section_urls, log_placeholder)

    if stats is not None:
        st.success(
            "Готово. "
            f"Карточек проверено: {stats['checked']} | "
            f"В наличии: {stats['in_stock']} | "
            f"Пропущено (нет в наличии): {stats['skipped_not_in_stock']} | "
            f"Ошибок: {stats['errors']} | Файлов XLSX: {stats['files']}"
        )

        if not files_payload:
            st.info("Нет данных для формирования отчетов.")
        elif len(files_payload) == 1:
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

    # Пояснение: логи отображаются внизу интерфейса.
    st.subheader("Логи обработки")
    bottom_log_placeholder = st.empty()
    render_logs(bottom_log_placeholder)

if __name__ == "__main__":
    main()
