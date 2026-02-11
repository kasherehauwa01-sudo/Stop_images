from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Dict, List, Set, Tuple
from urllib.parse import urljoin, urlparse

import requests
import streamlit as st
from bs4 import BeautifulSoup

from config import read_secret_or_env
from excel_io import build_report, make_rows_from_excel, make_rows_from_manual_input, read_excel
from mapping import FIELD_LABELS, REQUIRED_FIELDS_SYNONYMS, auto_map_columns
from storage import Storage
from tineye_client import TinEyeScraperClient, TinEyeScraperSettings

DB_PATH = Path("cache.db")
MAX_FILE_SIZE = 25 * 1024 * 1024
TIMEOUT_SECONDS = 20
DEFAULT_TOP_N = 20


def get_image_hash(image_url: str) -> str:
    # Пояснение: кэшируем по хэшу изображения; fallback на URL+ETag/Last-Modified,
    # если бинарник недоступен.
    headers = {"User-Agent": "StopImages/2.0"}
    try:
        resp = requests.get(image_url, timeout=TIMEOUT_SECONDS, stream=True, allow_redirects=True, headers=headers)
        resp.raise_for_status()
        content_type = resp.headers.get("content-type", "").lower()
        if not content_type.startswith("image/"):
            raise ValueError("URL не указывает на image/*")

        total = 0
        digest = hashlib.sha256()
        for chunk in resp.iter_content(chunk_size=1024 * 128):
            if not chunk:
                continue
            total += len(chunk)
            if total > MAX_FILE_SIZE:
                raise ValueError("Размер изображения превышает 25 МБ")
            digest.update(chunk)
        return digest.hexdigest()
    except Exception:
        head = requests.head(image_url, timeout=TIMEOUT_SECONDS, allow_redirects=True, headers=headers)
        fallback_base = f"{image_url}|{head.headers.get('ETag','')}|{head.headers.get('Last-Modified','')}"
        return hashlib.sha256(fallback_base.encode("utf-8")).hexdigest()


def make_row_key(product_url: str, src_index: int) -> str:
    return hashlib.sha256(f"{src_index}|{product_url}".encode("utf-8")).hexdigest()


def download_image_bytes(image_url: str) -> Tuple[bytes, str, str]:
    # Пояснение: загружаем бинарник изображения для последующей загрузки в веб-интерфейс TinEye.
    headers = {"User-Agent": "StopImages/2.0"}
    resp = requests.get(image_url, timeout=TIMEOUT_SECONDS, stream=True, allow_redirects=True, headers=headers)
    resp.raise_for_status()
    content_type = resp.headers.get("content-type", "").split(";")[0].strip().lower()
    if not content_type.startswith("image/"):
        raise ValueError(f"Ресурс не image/*: {content_type}")

    chunks = []
    total = 0
    for chunk in resp.iter_content(chunk_size=1024 * 128):
        if not chunk:
            continue
        total += len(chunk)
        if total > MAX_FILE_SIZE:
            raise ValueError("Размер изображения превышает 25 МБ")
        chunks.append(chunk)

    image_bytes = b"".join(chunks)
    ext = content_type.split("/")[-1] or "jpg"
    filename = f"upload_image.{ext}"
    return image_bytes, filename, content_type

def _extract_product_image_candidates(soup: BeautifulSoup, product_url: str) -> List[str]:
    # Пояснение: собираем кандидаты изображения из мета-тегов, JSON-LD и типовых галерей карточки.
    candidates: List[str] = []

    for meta_selector in [
        "meta[property='og:image']",
        "meta[name='twitter:image']",
        "meta[itemprop='image']",
    ]:
        node = soup.select_one(meta_selector)
        if node and node.get("content"):
            candidates.append(urljoin(product_url, node.get("content", "").strip()))

    for script in soup.select("script[type='application/ld+json']"):
        text = (script.string or script.get_text() or "").strip()
        if not text:
            continue
        try:
            import json

            payload = json.loads(text)
        except Exception:
            continue

        stack = [payload]
        while stack:
            item = stack.pop()
            if isinstance(item, dict):
                for k, v in item.items():
                    if k == "image":
                        if isinstance(v, str):
                            candidates.append(urljoin(product_url, v))
                        elif isinstance(v, list):
                            for vv in v:
                                if isinstance(vv, str):
                                    candidates.append(urljoin(product_url, vv))
                    else:
                        stack.append(v)
            elif isinstance(item, list):
                stack.extend(item)

    gallery_selectors = [
        "img[itemprop='image']",
        ".product-gallery img",
        ".product-images img",
        ".product-card img",
        "img",
    ]
    for selector in gallery_selectors:
        for img in soup.select(selector):
            src = (img.get("src") or img.get("data-src") or img.get("data-original") or "").strip()
            if not src:
                continue
            full = urljoin(product_url, src)
            candidates.append(full)

    # де-дупликат с сохранением порядка
    uniq: List[str] = []
    seen = set()
    for c in candidates:
        if c not in seen:
            seen.add(c)
            uniq.append(c)
    return uniq


def extract_article_and_image_from_product_page(product_url: str) -> Tuple[str, str]:
    # Пояснение: с карточки товара извлекаем артикул и главное изображение.
    headers = {"User-Agent": "Mozilla/5.0 StopImages/2.0"}
    resp = requests.get(product_url, timeout=TIMEOUT_SECONDS, allow_redirects=True, headers=headers)
    resp.raise_for_status()

    if resp.headers.get("content-type", "").lower().startswith("image/"):
        return "", product_url

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

    # Пояснение: отдаем первый релевантный кандидат; при необходимости кэш компенсирует повторы.
    image_url = image_candidates[0]
    return article, image_url


def scrape_product_links_from_section(section_url: str) -> List[str]:
    # Пояснение: со страницы раздела собираем ссылки на карточки товаров.
    headers = {"User-Agent": "Mozilla/5.0 StopImages/2.0"}
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
    seen: Set[str] = set()

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
            # Фильтруем очевидно не-карточки.
            if any(x in parsed.path.lower() for x in ["/catalog/", "/product/", "/item/"]):
                if full not in seen:
                    seen.add(full)
                    links.append(full)

    return links






def build_tineye_search_url(base_url: str, image_url: str) -> str:
    # Пояснение: формируем наглядный URL запроса TinEye для блока в интерфейсе.
    from urllib.parse import urlencode

    return f"{base_url.rstrip('/')}/search?" + urlencode({"url": image_url})


def render_tineye_request_url(url_placeholder, url_value: str) -> None:
    # Пояснение: отдельный блок UI, где явно виден сформированный запрос к TinEye.
    if not url_value:
        url_placeholder.code("TinEye URL пока не сформирован.", language="text")
    else:
        url_placeholder.code(url_value, language="text")



def set_last_tineye_rows(rows: List[Dict[str, str]]) -> None:
    # Пояснение: сохраняем последние найденные ссылки TinEye для наглядного показа в UI.
    st.session_state["last_tineye_rows"] = rows


def render_last_tineye_rows() -> None:
    # Пояснение: отдельный диагностический блок со списком распарсенных ссылок TinEye.
    rows = st.session_state.get("last_tineye_rows", [])
    st.subheader("Что удалось распарсить из TinEye URL")
    if rows:
        st.dataframe(rows, use_container_width=True)
    else:
        st.caption("Пока нет данных для отображения.")

def init_logs() -> None:
    # Пояснение: логи храним в session_state, чтобы пользователь видел ход обработки в UI.
    if "ui_logs" not in st.session_state:
        st.session_state["ui_logs"] = []


def append_log(message: str) -> None:
    # Пояснение: централизованное добавление строк лога с ограничением размера буфера.
    logs = st.session_state.get("ui_logs", [])
    logs.append(message)
    st.session_state["ui_logs"] = logs[-500:]


def render_logs(log_placeholder) -> None:
    # Пояснение: показываем последние строки лога в отдельном блоке интерфейса.
    logs = st.session_state.get("ui_logs", [])
    text = "\n".join(logs[-200:]) if logs else "Логи пока отсутствуют."
    log_placeholder.code(text, language="text")

def show_help() -> None:
    with st.expander("Как работает механика проверки", expanded=False):
        # Пояснение: объясняем эквивалент механики "Search image on TinEye" в автоматическом режиме.
        st.markdown(
            """
1. Вы даете ссылку на раздел сайта.
2. Приложение скрапит карточки товаров в разделе.
3. Для каждой карточки извлекается изображение и выполняется поиск в TinEye.
4. Это программный эквивалент ручного пункта **Search image on TinEye** из контекстного меню браузера.
5. В отчет попадают только совпадения на доменах из `stocks_config.json`.
            """
        )


def process_batch(
    products: List[Dict[str, str]],
    storage: Storage,
    tineye_client: TinEyeScraperClient,
    batch_size: int,
    log_placeholder,
    tineye_url_placeholder,
):
    # Пояснение: стартовая карточка фиксирована с первой позиции по требованию.
    start_idx = 0
    end_idx = min(len(products), start_idx + batch_size)
    batch = list(enumerate(products[start_idx:end_idx], start=start_idx))

    run_id = storage.create_batch_run(start_row=1, batch_size=batch_size, top_n=DEFAULT_TOP_N)
    progress = st.progress(0.0)
    status_box = st.empty()

    processed_count = 0
    error_count = 0
    matched_count = 0
    batch_keys: List[str] = []

    for i, (src_index, product) in enumerate(batch, start=1):
        product_url = product["product_url"]
        row_key = make_row_key(product_url, src_index)
        batch_keys.append(row_key)
        status_msg = f"Обработка карточки {src_index + 1} ({i}/{len(batch)})"
        status_box.info(status_msg)
        append_log(status_msg)
        render_logs(log_placeholder)

        try:
            article, image_url = extract_article_and_image_from_product_page(product_url)
            append_log(f"Извлечено изображение: {image_url}")
            image_bytes, _, _ = download_image_bytes(image_url)
            image_hash = hashlib.sha256(image_bytes).hexdigest()
            cached = storage.get_cached_results(image_hash)
            if cached is None:
                append_log(f"TinEye url-запрос с извлеченным image_url: {image_url}")
                tineye_request_url = build_tineye_search_url(tineye_client.settings.base_url, image_url)
                render_tineye_request_url(tineye_url_placeholder, tineye_request_url)
                # Пояснение: в TinEye передаем именно извлеченный URL изображения, а не URL карточки.
                tineye_results = tineye_client.search_by_url(image_url, top_n=DEFAULT_TOP_N)
                storage.set_cached_results(image_hash, tineye_results)
                append_log(f"Кэш сохранен: {image_hash[:12]}")
            else:
                tineye_results = cached
                append_log(f"Использован кэш: {image_hash[:12]}")

            report_rows = []
            parsed_rows_for_ui = []
            for result in tineye_results:
                result_url = result.get("page_url", "")
                append_log(f"TinEye результат: {result_url}")
                if result_url:
                    parsed_rows_for_ui.append({"Ссылка в результатах TinEye": result_url})
                    report_rows.append(
                        {
                            "Артикул товара": article,
                            "Ссылка на сайт": product_url,
                            "Ссылка в результатах TinEye": result_url,
                        }
                    )

            set_last_tineye_rows(parsed_rows_for_ui)
            storage.upsert_row_status(row_key, "done", image_hash, len(report_rows), None)
            if report_rows:
                storage.add_report_rows(row_key, report_rows)
                matched_count += len(report_rows)
                append_log(f"Найдено результатов TinEye: {len(report_rows)} | {product_url}")
            else:
                append_log(f"Результаты TinEye не найдены | {product_url}")
            processed_count += 1

        except Exception as exc:
            storage.upsert_row_status(row_key, "error", None, 0, str(exc))
            append_log(f"Ошибка: {product_url} | {exc}")
            error_count += 1
            processed_count += 1

        progress_line = f"Обработано: {processed_count} | В очереди: {len(batch)-processed_count} | Ошибки: {error_count} | Совпадения: {matched_count}"
        st.caption(progress_line)
        append_log(progress_line)
        render_logs(log_placeholder)
        progress.progress(i / max(1, len(batch)))

    storage.finish_batch_run(run_id, processed_count, error_count, matched_count)
    return storage.get_report_rows_for_keys(batch_keys), {"processed": processed_count, "errors": error_count, "matches": matched_count}


def check_single_url(
    source_url: str,
    storage: Storage,
    tineye_client: TinEyeScraperClient,
    tineye_url_placeholder,
) -> List[Dict[str, str]]:
    # Пояснение: вкладка "Проверка URL" проверяет одно URL страницы без пакетного режима.
    source_url = source_url.strip()
    if not source_url:
        return []

    article, image_url = extract_article_and_image_from_product_page(source_url)
    append_log(f"Одиночная проверка: извлечено изображение {image_url}")
    image_bytes, _, _ = download_image_bytes(image_url)
    image_hash = hashlib.sha256(image_bytes).hexdigest()
    cached = storage.get_cached_results(image_hash)
    if cached is None:
        append_log(f"TinEye url-запрос для одиночной проверки с image_url: {image_url}")
        tineye_request_url = build_tineye_search_url(tineye_client.settings.base_url, image_url)
        render_tineye_request_url(tineye_url_placeholder, tineye_request_url)
        # Пояснение: в TinEye передаем именно извлеченный URL изображения, а не исходный URL страницы.
        results = tineye_client.search_by_url(image_url, top_n=DEFAULT_TOP_N)
        storage.set_cached_results(image_hash, results)
    else:
        append_log(f"Одиночная проверка: использован кэш {image_hash[:12]}")
        results = cached

    rows: List[Dict[str, str]] = []
    parsed_rows_for_ui: List[Dict[str, str]] = []
    for item in results:
        result_url = item.get("page_url", "")
        append_log(f"TinEye результат: {result_url}")
        if result_url:
            parsed_rows_for_ui.append({"Ссылка в результатах TinEye": result_url})
            rows.append({
                "Артикул товара": article,
                "Ссылка на сайт": source_url,
                "Ссылка в результатах TinEye": result_url,
            })
    set_last_tineye_rows(parsed_rows_for_ui)
    return rows


def main() -> None:
    st.set_page_config(page_title="Проверка раздела сайта через TinEye", layout="wide")
    st.title("Проверка карточек товаров из раздела сайта")

    storage = Storage(DB_PATH)
    tineye_client = TinEyeScraperClient(
        TinEyeScraperSettings(base_url=read_secret_or_env("TINEYE_BASE_URL", "https://tineye.com"), timeout_seconds=30)
    )

    init_logs()
    show_help()

    st.subheader("Логи обработки")
    log_placeholder = st.empty()
    render_logs(log_placeholder)

    st.subheader("Сформированный TinEye URL запроса")
    tineye_url_placeholder = st.empty()
    render_tineye_request_url(tineye_url_placeholder, "")

    render_last_tineye_rows()

    tab_batch, tab_single = st.tabs(["Пакетная проверка разделов", "Проверка URL"])

    with tab_batch:
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

        products: List[Dict[str, str]] = []
        if source_df is not None:
            section_urls = [
                str(r.get("input_url", "")).strip()
                for r in source_df.fillna("").to_dict(orient="records")
                if str(r.get("input_url", "")).strip()
            ]
            if section_urls:
                with st.expander("Найденные карточки товаров", expanded=False):
                    for section_url in section_urls:
                        try:
                            links = scrape_product_links_from_section(section_url)
                            line = f"{section_url} → найдено карточек: {len(links)}"
                            st.write(line)
                            append_log(line)
                            render_logs(log_placeholder)
                            products.extend({"product_url": link} for link in links)
                        except Exception as exc:
                            warn = f"Ошибка парсинга раздела {section_url}: {exc}"
                            st.warning(warn)
                            append_log(warn)
                            render_logs(log_placeholder)

        uniq = []
        seen = set()
        for item in products:
            if item["product_url"] not in seen:
                seen.add(item["product_url"])
                uniq.append(item)
        products = uniq

        st.info(f"Всего карточек к проверке: {len(products)}")

        batch_size = st.selectbox("Размер партии", [50, 100, 200], index=0)
        run = st.button("Запустить партию", type="primary", disabled=(len(products) == 0), key="run_batch")

    with tab_single:
        # Пояснение: отдельная вкладка для проверки одной страницы по URL.
        one_url = st.text_input("Введите URL страницы товара")
        run_one = st.button("Проверить URL", type="primary", key="run_one")
        if run_one:
            st.session_state["ui_logs"] = []
            append_log(f"Старт проверки одного URL: {one_url}")
            render_logs(log_placeholder)
            try:
                rows = check_single_url(one_url, storage, tineye_client, tineye_url_placeholder)
                if rows:
                    st.success(f"Найдено результатов TinEye: {len(rows)}")
                    st.dataframe(rows, use_container_width=True)
                    st.download_button(
                        label="Скачать XLSX-отчет по URL",
                        data=build_report(rows),
                        file_name="single_url_report.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="download_one",
                    )
                else:
                    st.info("Результаты TinEye не найдены.")
                append_log("Одиночная проверка завершена")
                render_logs(log_placeholder)
            except Exception as exc:
                st.error(f"Ошибка проверки URL: {exc}")
                append_log(f"Ошибка одиночной проверки: {exc}")
                render_logs(log_placeholder)

    if run:
        st.session_state["ui_logs"] = []
        append_log("Старт пакетной обработки")
        render_logs(log_placeholder)
        report_records, stats = process_batch(
            products=products,
            storage=storage,
            tineye_client=tineye_client,
            batch_size=int(batch_size),
            log_placeholder=log_placeholder,
            tineye_url_placeholder=tineye_url_placeholder,
        )

        st.success(
            f"Партия завершена: обработано {stats['processed']}, ошибок {stats['errors']}, совпадений {stats['matches']}"
        )
        report_bytes = build_report(report_records)
        st.download_button(
            label="Скачать XLSX-отчет",
            data=report_bytes,
            file_name=f"stock_report_size{int(batch_size)}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


if __name__ == "__main__":
    main()
