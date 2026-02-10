import datetime as dt
import hashlib
import io
import json
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup

DB_PATH = Path("cache.db")
CONFIG_PATH = Path("stocks_config.json")
MAX_FILE_SIZE = 25 * 1024 * 1024
TIMEOUT_SECONDS = 20

# Пояснение: словарь синонимов нужен для автоматического сопоставления колонок,
# чтобы принимать разные варианты названий из исходных файлов.
REQUIRED_FIELDS_SYNONYMS = {
    "image_code": ["код изображения", "image_code", "code", "id", "артикул", "sku"],
    "image_url": [
        "ссылка на изображение",
        "image_url",
        "url",
        "link",
        "image",
        "фото",
    ],
    "product_name": ["наименование товара", "товар", "product", "product_name", "name"],
    "supplier": ["поставщик", "supplier", "vendor"],
    "manager": ["менеджер", "manager", "ответственный"],
}

# Пояснение: читаемые подписи для интерфейса ручного сопоставления.
FIELD_LABELS = {
    "image_code": "Код изображения",
    "image_url": "Ссылка на изображение (URL)",
    "product_name": "Наименование товара",
    "supplier": "Поставщик",
    "manager": "Менеджер",
}


@dataclass
class StockRule:
    source_type: str
    host: str
    path_prefix: str


class StockMatcher:
    # Пояснение: правила загружаются из конфигурации, поэтому список доменов можно менять без правок логики.
    def __init__(self, config_path: Path):
        with config_path.open("r", encoding="utf-8") as f:
            config = json.load(f)

        self.rules: List[StockRule] = []
        for source_type, entries in config.items():
            for entry in entries:
                parsed = urlparse(f"https://{entry}")
                host = parsed.netloc.lower()
                path_prefix = parsed.path.rstrip("/")
                self.rules.append(StockRule(source_type=source_type, host=host, path_prefix=path_prefix))

    def classify(self, candidate_url: str) -> Optional[Tuple[str, str]]:
        parsed = urlparse(candidate_url)
        host = parsed.netloc.lower()
        path = parsed.path.rstrip("/")

        for rule in self.rules:
            # Пояснение: разрешаем совпадения как по точному домену, так и по поддоменам.
            if host == rule.host or host.endswith(f".{rule.host}"):
                if rule.path_prefix and not path.startswith(rule.path_prefix):
                    continue
                return candidate_url, rule.source_type
        return None


class CacheDB:
    # Пояснение: SQLite нужен для кэша результатов reverse search по хэшу изображения.
    def __init__(self, db_path: Path):
        self.conn = sqlite3.connect(db_path)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS image_cache (
                image_hash TEXT PRIMARY KEY,
                search_results_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        self.conn.commit()

    def get(self, image_hash: str) -> Optional[List[str]]:
        row = self.conn.execute(
            "SELECT search_results_json FROM image_cache WHERE image_hash = ?", (image_hash,)
        ).fetchone()
        if not row:
            return None
        return json.loads(row[0])

    def set(self, image_hash: str, search_results: List[str]) -> None:
        self.conn.execute(
            """
            INSERT INTO image_cache (image_hash, search_results_json, created_at)
            VALUES (?, ?, ?)
            ON CONFLICT(image_hash) DO UPDATE SET
                search_results_json = excluded.search_results_json,
                created_at = excluded.created_at
            """,
            (image_hash, json.dumps(search_results, ensure_ascii=False), dt.datetime.utcnow().isoformat()),
        )
        self.conn.commit()


def normalize_text(value: str) -> str:
    cleaned = value.strip().lower().replace("ё", "е")
    return re.sub(r"\s+", "", cleaned)


def extract_urls_from_json(payload) -> List[str]:
    urls: List[str] = []

    def walk(node):
        if isinstance(node, dict):
            for key, val in node.items():
                if key in {"hostPageUrl", "contentUrl", "webSearchUrl", "url"} and isinstance(val, str):
                    if val.startswith("http"):
                        urls.append(val)
                walk(val)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(payload)
    return list(dict.fromkeys(urls))


def fetch_image_bytes_from_url(url: str) -> bytes:
    headers = {"User-Agent": "Mozilla/5.0 StopImages/1.0"}
    response = requests.get(url, timeout=TIMEOUT_SECONDS, allow_redirects=True, headers=headers, stream=True)
    response.raise_for_status()
    content_type = response.headers.get("content-type", "").lower()

    if content_type.startswith("image/"):
        content = response.content
        if len(content) > MAX_FILE_SIZE:
            raise ValueError("Размер изображения превышает 25 МБ")
        return content

    if "text/html" in content_type:
        html = response.text
        img_url = extract_image_url_from_html(url, html)
        if not img_url:
            raise ValueError("Не удалось найти изображение на HTML-странице")
        image_resp = requests.get(
            img_url, timeout=TIMEOUT_SECONDS, allow_redirects=True, headers=headers, stream=True
        )
        image_resp.raise_for_status()
        image_ct = image_resp.headers.get("content-type", "").lower()
        if not image_ct.startswith("image/"):
            raise ValueError("Найденный ресурс не является изображением")
        content = image_resp.content
        if len(content) > MAX_FILE_SIZE:
            raise ValueError("Размер изображения превышает 25 МБ")
        return content

    raise ValueError(f"Неподдерживаемый content-type: {content_type}")


def extract_image_url_from_html(base_url: str, html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")

    og_image = soup.find("meta", attrs={"property": "og:image"})
    if og_image and og_image.get("content"):
        return urljoin(base_url, og_image["content"])

    best_src = None
    best_score = -1
    for img in soup.find_all("img"):
        src = img.get("src")
        if not src:
            continue
        width = int(img.get("width") or 0)
        height = int(img.get("height") or 0)
        score = width * height
        if score > best_score:
            best_score = score
            best_src = src

    if best_src:
        return urljoin(base_url, best_src)
    return None


def bing_visual_search(image_bytes: bytes, api_key: str, endpoint: str) -> List[str]:
    headers = {"Ocp-Apim-Subscription-Key": api_key}
    files = {"image": ("image.jpg", image_bytes, "image/jpeg")}
    response = requests.post(endpoint, headers=headers, files=files, timeout=TIMEOUT_SECONDS)
    response.raise_for_status()
    payload = response.json()
    return extract_urls_from_json(payload)


def auto_map_columns(columns: Iterable[str]) -> Tuple[Dict[str, Optional[str]], bool]:
    normalized_columns = {col: normalize_text(col) for col in columns}
    mapping: Dict[str, Optional[str]] = {}
    needs_manual = False

    for field, synonyms in REQUIRED_FIELDS_SYNONYMS.items():
        normalized_synonyms = {normalize_text(s) for s in synonyms}
        matched = [col for col, normalized in normalized_columns.items() if normalized in normalized_synonyms]
        if len(matched) == 1:
            mapping[field] = matched[0]
        else:
            mapping[field] = None
            needs_manual = True

    return mapping, needs_manual


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
    # Пояснение: значения берем напрямую из XLS без трансформаций, как требует ТЗ.
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


def main() -> None:
    st.set_page_config(page_title="Проверка изображений на фотостоках", layout="wide")
    st.title("Проверка изображений по URL и поиск совпадений на фотостоках")

    cache = CacheDB(DB_PATH)
    matcher = StockMatcher(CONFIG_PATH)

    # Пояснение: API-ключ и endpoint выносим в настройки окружения Streamlit,
    # чтобы не хранить секреты в коде.
    api_key = st.secrets.get("BING_API_KEY", "")
    endpoint = st.secrets.get(
        "BING_VISUAL_SEARCH_ENDPOINT", "https://api.bing.microsoft.com/v7.0/images/visualsearch"
    )

    if not api_key:
        st.warning("Не задан BING_API_KEY в настройках Streamlit secrets.")

    input_mode = st.radio("Способ ввода", ["Ручной ввод URL", "Загрузка XLS"], horizontal=True)

    source_rows = None
    mapping_confirmed: Dict[str, str] = {}

    if input_mode == "Ручной ввод URL":
        manual_text = st.text_area("Введите ссылки на изображения (по одной на строку)", height=220)
        source_rows = make_rows_from_manual_input(manual_text)
        mapping_confirmed = {
            "image_code": "image_code",
            "image_url": "image_url",
            "product_name": "product_name",
            "supplier": "supplier",
            "manager": "manager",
        }
        st.caption(f"Подготовлено URL: {len(source_rows)}")
    else:
        upload = st.file_uploader("Загрузите XLS/XLSX", type=["xls", "xlsx"])
        if upload is not None:
            df_raw = pd.read_excel(upload)
            st.write("Предпросмотр загруженного файла:")
            st.dataframe(df_raw.head(10), use_container_width=True)

            auto_mapping, needs_manual = auto_map_columns(df_raw.columns)
            st.subheader("Сопоставление колонок")

            if not needs_manual:
                mapping_confirmed = {k: v for k, v in auto_mapping.items() if v is not None}
                st.success("Автоматическое сопоставление выполнено успешно.")
                st.json(mapping_confirmed)
            else:
                st.warning("Требуется ручное сопоставление. Подтвердите все обязательные поля.")
                options = [""] + list(df_raw.columns)
                for field in REQUIRED_FIELDS_SYNONYMS.keys():
                    default = auto_mapping[field] if auto_mapping[field] else ""
                    selected = st.selectbox(
                        FIELD_LABELS[field],
                        options=options,
                        index=options.index(default) if default in options else 0,
                        key=f"mapping_{field}",
                    )
                    if selected:
                        mapping_confirmed[field] = selected

            if len(mapping_confirmed) == len(REQUIRED_FIELDS_SYNONYMS):
                source_rows = make_rows_from_excel(df_raw, mapping_confirmed)
            else:
                source_rows = None

    run = st.button("Запустить проверку", type="primary", disabled=(source_rows is None or source_rows.empty))

    if run:
        if not api_key:
            st.error("Нельзя запустить проверку без BING_API_KEY")
            return

        results_for_report: List[Dict[str, str]] = []
        progress = st.progress(0.0)
        status = st.empty()

        total = len(source_rows)
        for idx, row in source_rows.iterrows():
            status.info(f"Обработка {idx + 1}/{total}")
            image_url = str(row["image_url"]).strip()

            if not image_url:
                progress.progress((idx + 1) / total)
                continue

            try:
                image_bytes = fetch_image_bytes_from_url(image_url)
                image_hash = hashlib.sha256(image_bytes).hexdigest()

                cached = cache.get(image_hash)
                if cached is not None:
                    candidate_urls = cached
                else:
                    candidate_urls = bing_visual_search(image_bytes, api_key, endpoint)
                    cache.set(image_hash, candidate_urls)

                matched_rows = []
                for candidate in candidate_urls:
                    matched = matcher.classify(candidate)
                    if matched:
                        stock_url, source_type = matched
                        matched_rows.append((stock_url, source_type))

                for stock_url, source_type in matched_rows:
                    results_for_report.append(
                        {
                            "Дата проверки": dt.datetime.now().strftime("%d.%m.%Y %H:%M"),
                            "Код изображения": row["image_code"],
                            "Наименование товара": row["product_name"],
                            "Поставщик": row["supplier"],
                            "Менеджер": row["manager"],
                            "Ссылка на изображение": row["image_url"],
                            "Ссылка на сток": stock_url,
                            "Тип источника": source_type,
                        }
                    )

            except Exception as exc:
                st.warning(f"Ошибка для URL '{image_url}': {exc}")

            progress.progress((idx + 1) / total)

        report_bytes = build_report(results_for_report)
        status.success(f"Обработка завершена. Строк в отчете: {len(results_for_report)}")

        st.download_button(
            label="Скачать итоговый XLSX-отчет",
            data=report_bytes,
            file_name=f"stock_report_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


if __name__ == "__main__":
    main()
