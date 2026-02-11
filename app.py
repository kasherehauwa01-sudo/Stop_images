from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import urljoin

import requests
import streamlit as st
from bs4 import BeautifulSoup

from config import classify_stock_url, load_stock_rules, read_secret_or_env
from excel_io import build_report, make_rows_from_excel, make_rows_from_manual_input, read_excel
from mapping import FIELD_LABELS, REQUIRED_FIELDS_SYNONYMS, auto_map_columns
from storage import Storage
from tineye_client import TinEyeScraperClient, TinEyeScraperSettings

DB_PATH = Path("cache.db")
CONFIG_PATH = Path("stocks_config.json")
MAX_FILE_SIZE = 25 * 1024 * 1024
TIMEOUT_SECONDS = 20


def get_image_hash(image_url: str) -> str:
    # Пояснение: сначала считаем хэш по бинарному контенту картинки,
    # если это не удалось — fallback по URL и заголовкам кеширования.
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
        etag = head.headers.get("ETag", "")
        last_modified = head.headers.get("Last-Modified", "")
        fallback_base = f"{image_url}|{etag}|{last_modified}"
        return hashlib.sha256(fallback_base.encode("utf-8")).hexdigest()


def make_row_key(source_url: str, src_index: int) -> str:
    base = f"{src_index}|{source_url}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def extract_image_and_article_from_page(page_url: str) -> Tuple[str, str]:
    # Пояснение: из входной страницы извлекаем URL основного изображения и артикул товара.
    headers = {"User-Agent": "Mozilla/5.0 StopImages/2.0"}
    resp = requests.get(page_url, timeout=TIMEOUT_SECONDS, allow_redirects=True, headers=headers)
    resp.raise_for_status()

    content_type = resp.headers.get("content-type", "").lower()
    if content_type.startswith("image/"):
        return page_url, ""

    soup = BeautifulSoup(resp.text, "html.parser")

    og_image = soup.find("meta", attrs={"property": "og:image"})
    if og_image and og_image.get("content"):
        image_url = urljoin(page_url, og_image.get("content", ""))
    else:
        best_img = ""
        best_score = -1
        for img in soup.find_all("img"):
            src = (img.get("src") or "").strip()
            if not src:
                continue
            width = int(img.get("width") or 0)
            height = int(img.get("height") or 0)
            score = width * height
            if score > best_score:
                best_score = score
                best_img = src
        if not best_img:
            raise ValueError("Не найдено изображение на странице")
        image_url = urljoin(page_url, best_img)

    article = ""
    sku_meta = soup.find("meta", attrs={"property": "product:retailer_item_id"}) or soup.find(
        "meta", attrs={"name": "sku"}
    )
    if sku_meta and sku_meta.get("content"):
        article = sku_meta.get("content", "").strip()

    if not article:
        selectors = [
            "[itemprop='sku']",
            ".sku",
            "#sku",
            "[data-sku]",
            ".product-sku",
            ".article",
            "#article",
        ]
        for selector in selectors:
            node = soup.select_one(selector)
            if node:
                article = (node.get("data-sku") or node.get_text(" ", strip=True) or "").strip()
                if article:
                    break

    return image_url, article


def show_tineye_help() -> None:
    with st.expander("Параметры скрапинга TinEye", expanded=False):
        # Пояснение: настройки скрапинга для запуска на Streamlit Cloud без API-ключей.
        st.markdown(
            """
Приложение работает в режиме **скрапинга страницы результатов TinEye**.

Добавьте (опционально) в `.streamlit/secrets.toml`:

```toml
TINEYE_BASE_URL = "https://tineye.com"
```

Если параметр не задан, используется `https://tineye.com`.
            """
        )


def process_batch(
    rows: List[Dict[str, str]],
    storage: Storage,
    tineye_client: TinEyeScraperClient,
    stock_rules,
    start_row: int,
    batch_size: int,
    top_n: int,
):
    start_idx = max(1, start_row) - 1
    end_idx = min(len(rows), start_idx + batch_size)
    batch = list(enumerate(rows[start_idx:end_idx], start=start_idx))

    run_id = storage.create_batch_run(start_row=start_row, batch_size=batch_size, top_n=top_n)
    progress = st.progress(0.0)
    status_box = st.empty()

    processed_count = 0
    error_count = 0
    matched_count = 0
    batch_keys: List[str] = []

    for i, (src_index, row) in enumerate(batch, start=1):
        source_url = str(row.get("image_url", "")).strip()
        row_key = make_row_key(source_url, src_index)
        batch_keys.append(row_key)

        status_box.info(f"Обработка строки {src_index + 1} ({i}/{len(batch)})")

        if not source_url:
            storage.upsert_row_status(row_key, status="error", image_hash=None, matches_count=0, error_text="Пустая ссылка")
            error_count += 1
            processed_count += 1
            progress.progress(i / max(1, len(batch)))
            continue

        try:
            image_url, article = extract_image_and_article_from_page(source_url)
            image_hash = get_image_hash(image_url)
            cached_results = storage.get_cached_results(image_hash)
            if cached_results is None:
                tineye_results = tineye_client.search_by_url(image_url, top_n=top_n)
                storage.set_cached_results(image_hash, tineye_results)
            else:
                tineye_results = cached_results

            report_rows = []
            for result in tineye_results:
                page_url = result.get("page_url", "")
                matched = classify_stock_url(page_url, stock_rules)
                if matched:
                    stock_url, _ = matched
                    report_rows.append({"Ссылка на сток": stock_url, "Артикул товара": article})

            storage.upsert_row_status(
                row_key,
                status="done",
                image_hash=image_hash,
                matches_count=len(report_rows),
                error_text=None,
            )
            if report_rows:
                storage.add_report_rows(row_key, report_rows)
                matched_count += len(report_rows)

            processed_count += 1
        except Exception as exc:
            storage.upsert_row_status(
                row_key,
                status="error",
                image_hash=None,
                matches_count=0,
                error_text=str(exc),
            )
            error_count += 1
            processed_count += 1

        st.caption(
            f"Обработано: {processed_count} | В очереди: {len(batch)-processed_count} | Ошибки: {error_count} | Совпадения: {matched_count}"
        )
        progress.progress(i / max(1, len(batch)))

    storage.finish_batch_run(run_id, processed_count=processed_count, error_count=error_count, match_count=matched_count)
    report_records = storage.get_report_rows_for_keys(batch_keys)
    return report_records, {"processed": processed_count, "errors": error_count, "matches": matched_count}


def main() -> None:
    st.set_page_config(page_title="Проверка изображений со страниц сайта", layout="wide")
    st.title("Проверка изображения по ссылке на страницу и выгрузка отчета")

    storage = Storage(DB_PATH)
    stock_rules = load_stock_rules(CONFIG_PATH)

    settings = TinEyeScraperSettings(
        base_url=read_secret_or_env("TINEYE_BASE_URL", "https://tineye.com"),
        timeout_seconds=30,
    )
    tineye_client = TinEyeScraperClient(settings=settings)
    show_tineye_help()

    input_mode = st.radio("Способ ввода", ["Ручной ввод URL", "Загрузка XLS"], horizontal=True)
    source_df = None
    mapping_confirmed: Dict[str, str] = {}

    if input_mode == "Ручной ввод URL":
        text = st.text_area("Введите ссылки на страницы товаров (по одной на строку)", height=220)
        source_df = make_rows_from_manual_input(text)
        mapping_confirmed = {"image_url": "image_url"}
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
                st.warning("Автосопоставление неполное: укажите колонку со ссылкой вручную.")
                options = [""] + list(df_raw.columns)
                field = "image_url"
                default = auto_mapping.get(field) or ""
                selected = st.selectbox(
                    FIELD_LABELS[field],
                    options=options,
                    index=options.index(default) if default in options else 0,
                    key="map_image_url",
                )
                if selected:
                    mapping_confirmed[field] = selected

            if len(mapping_confirmed) == len(REQUIRED_FIELDS_SYNONYMS):
                source_df = make_rows_from_excel(df_raw, mapping_confirmed)

    if source_df is None:
        st.stop()

    records = source_df.fillna("").to_dict(orient="records")
    st.subheader("Параметры партии")
    c1, c2, c3 = st.columns(3)
    with c1:
        batch_size = st.selectbox("Размер партии", [50, 100, 200], index=0)
    with c2:
        start_row = st.number_input("Стартовая строка (с 1)", min_value=1, value=1, step=1)
    with c3:
        top_n = st.number_input("top_n результатов на изображение", min_value=1, max_value=100, value=20, step=1)

    run = st.button("Запустить партию", type="primary", disabled=(len(records) == 0))
    if run:
        report_records, stats = process_batch(
            rows=records,
            storage=storage,
            tineye_client=tineye_client,
            stock_rules=stock_rules,
            start_row=int(start_row),
            batch_size=int(batch_size),
            top_n=int(top_n),
        )

        st.success(
            f"Партия завершена: обработано {stats['processed']}, ошибок {stats['errors']}, совпадений {stats['matches']}"
        )
        report_bytes = build_report(report_records)
        st.download_button(
            label="Скачать XLSX-отчет",
            data=report_bytes,
            file_name=f"batch_report_start{int(start_row)}_size{int(batch_size)}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


if __name__ == "__main__":
    main()
