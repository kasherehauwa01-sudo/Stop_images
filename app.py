from __future__ import annotations

import json
import re
import html
from datetime import datetime
from io import BytesIO
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse
from zipfile import ZIP_DEFLATED, ZipFile

import requests
import streamlit as st
from bs4 import BeautifulSoup, Tag

from excel_io import build_report, make_rows_from_excel, make_rows_from_manual_input, read_excel
from mapping import FIELD_LABELS, REQUIRED_FIELDS_SYNONYMS, auto_map_columns, normalize_text

TIMEOUT_SECONDS = 20
REPORT_CHUNK_SIZE = 50
USER_AGENT = "Mozilla/5.0 StopImages/4.0"

# Пояснение: признаки технических изображений, которые нужно исключать.
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
    "pixel",
]

# Пояснение: признаки товарных ссылок.
PRODUCT_PATH_HINTS = ["/catalog/", "/product/", "/item/"]

# Пояснение: признаки нерелевантных блоков (рекомендации/слайдеры/баннеры).
NON_CATALOG_BLOCK_HINTS = ["recommend", "related", "similar", "slider", "carousel", "banner"]

IN_STOCK_HINTS = ["в наличии", "есть в наличии", "in stock", "добавить в корзину", "купить"]
OUT_OF_STOCK_HINTS = ["нет в наличии", "под заказ", "ожидается", "out of stock", "нет на складе"]


class HttpClient:
    # Пояснение: клиент с кэшированием HTML/HEAD-запросов и нормальными заголовками.
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.page_cache: Dict[str, Dict[str, str]] = {}
        self.head_cache: Dict[str, Dict[str, str]] = {}

    def get_page(self, url: str, referer: str = "") -> Dict[str, str]:
        cache_key = f"GET::{url}::{referer}"
        if cache_key in self.page_cache:
            return self.page_cache[cache_key]

        headers = {"Referer": referer} if referer else None
        resp = self.session.get(url, timeout=TIMEOUT_SECONDS, allow_redirects=True, headers=headers)
        if resp.status_code != 200:
            raise ValueError(f"HTTP {resp.status_code} для {url}")

        payload = {
            "source_url": url,
            "final_url": resp.url,
            "status_code": str(resp.status_code),
            "content_type": resp.headers.get("content-type", ""),
            "html": resp.text,
        }
        self.page_cache[cache_key] = payload
        return payload

    def head_or_get_image(self, image_url: str, referer: str = "") -> Dict[str, str]:
        cache_key = f"HEAD::{image_url}::{referer}"
        if cache_key in self.head_cache:
            return self.head_cache[cache_key]

        headers = {"Referer": referer} if referer else None
        try:
            resp = self.session.head(image_url, timeout=10, allow_redirects=True, headers=headers)
            ct = resp.headers.get("content-type", "")
            status = resp.status_code
            if status == 403 and not referer:
                # Пояснение: если CDN режет доступ, повторяем с Referer домена.
                resp = self.session.head(
                    image_url,
                    timeout=10,
                    allow_redirects=True,
                    headers={"Referer": f"{urlparse(image_url).scheme}://{urlparse(image_url).netloc}"},
                )
                ct = resp.headers.get("content-type", "")
                status = resp.status_code
        except Exception:
            resp = self.session.get(image_url, timeout=10, stream=True, allow_redirects=True, headers=headers)
            status = resp.status_code
            ct = resp.headers.get("content-type", "")

        payload = {"status_code": str(status), "content_type": ct}
        self.head_cache[cache_key] = payload
        return payload


def append_log(message: str) -> None:
    # Пояснение: добавляем timestamp, чтобы в онлайн-логах было понятно, что происходит прямо сейчас.
    st.session_state.setdefault("ui_logs", [])
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {message}"
    st.session_state["ui_logs"] = (st.session_state["ui_logs"] + [line])[-1200:]


def log_step(message: str, placeholder=None) -> None:
    # Пояснение: единая точка для онлайн-логов — записали строку и сразу отрисовали.
    append_log(message)
    render_logs(placeholder)


def render_logs(placeholder=None) -> None:
    # Пояснение: логи показываем в отдельном прокручиваемом поле фиксированной высоты.
    logs = st.session_state.get("ui_logs", [])
    if placeholder is None:
        return

    if logs:
        # Пояснение: каждый лог выводится отдельной строкой.
        safe_text = "<br>".join(html.escape(line) for line in logs[-400:])
    else:
        safe_text = "Логи пока отсутствуют."

    placeholder.markdown(
        f"""
<div class="log-box"><div class="log-lines">{safe_text}</div></div>
""",
        unsafe_allow_html=True,
    )


def build_tineye_search_url(image_url: str, base_url: str = "https://tineye.com") -> str:
    return f"{base_url.rstrip('/')}/search?" + urlencode({"url": image_url})


def normalize_product_url(url: str) -> str:
    # Пояснение: нормализуем URL товара, чтобы дедупликация была стабильной.
    parsed = urlparse(url)
    clean_path = re.sub(r"/+", "/", parsed.path).rstrip("/")
    return urlunparse((parsed.scheme, parsed.netloc.lower(), clean_path, "", "", ""))


def pick_biggest_from_srcset(srcset: str, base_url: str) -> str:
    # Пояснение: из srcset выбираем кандидат с максимальной шириной.
    best_url = ""
    best_width = -1
    for part in srcset.split(","):
        item = part.strip()
        if not item:
            continue
        bits = item.split()
        cand = urljoin(base_url, bits[0])
        width = 0
        if len(bits) > 1 and bits[1].endswith("w"):
            try:
                width = int(bits[1][:-1])
            except Exception:
                width = 0
        if width > best_width:
            best_width = width
            best_url = cand
    return best_url


def extract_preview_from_card(card: Tag, page_url: str) -> str:
    # Пояснение: извлекаем превью товара из src/data-src/srcset/style.
    img = card.select_one("img")
    if img:
        srcset = (img.get("srcset") or "").strip()
        if srcset:
            cand = pick_biggest_from_srcset(srcset, page_url)
            if cand:
                return cand

        for key in ["data-src", "data-original", "src"]:
            raw = (img.get(key) or "").strip()
            if raw:
                return urljoin(page_url, raw)

    style_node = card.select_one("[style*='background-image']")
    if style_node:
        style = style_node.get("style", "")
        m = re.search(r"background-image\s*:\s*url\((['\"]?)(.*?)\1\)", style)
        if m:
            return urljoin(page_url, m.group(2))

    return ""


def looks_like_product_link(path: str, section_path: str = "") -> bool:
    # Пояснение: товарная карточка обычно глубже, чем сама категория, и чаще содержит ID в конце.
    lower = path.lower()
    if not any(h in lower for h in PRODUCT_PATH_HINTS):
        return False

    section_norm = section_path.rstrip("/").lower()
    path_norm = lower.rstrip("/")
    if section_norm and path_norm == section_norm:
        return False

    parts = [p for p in path_norm.split("/") if p]
    # минимальная глубина отсекает часть ссылок меню/разделов
    if len(parts) < 4:
        return False

    last = parts[-1]
    # Пояснение: на большинстве карточек есть числовой id в конце пути.
    if re.fullmatch(r"\d+", last):
        return True
    if re.search(r"\d", last):
        return True

    # fallback: допускаем URL с product/item в пути
    if any(k in path_norm for k in ["/product/", "/item/"]):
        return True
    return False


def container_signature(tag: Optional[Tag]) -> str:
    # Пояснение: подпись контейнера для поиска самого повторяющегося блока карточки.
    if tag is None:
        return ""
    classes = sorted(tag.get("class", []))[:3]
    ident = tag.get("id", "")
    return f"{tag.name}|{' '.join(classes)}|{ident}"


def find_best_container_for_link(link: Tag) -> Optional[Tag]:
    # Пояснение: ищем ближайший контейнер карточки вокруг ссылки на товар.
    current: Optional[Tag] = link
    for _ in range(6):
        if current is None or not isinstance(current, Tag):
            return None
        if current.name in {"article", "li", "div"}:
            classes = " ".join(current.get("class", [])).lower()
            if any(h in classes for h in NON_CATALOG_BLOCK_HINTS):
                return None
            if current.find("img") and (current.find(text=True) or current.select_one("[class*='price']")):
                return current
        current = current.parent if isinstance(current.parent, Tag) else None
    return None


def extract_name_from_card(card: Tag, link: Tag) -> str:
    # Пояснение: название берем из заголовков внутри карточки или текста ссылки.
    for sel in ["h1", "h2", "h3", "h4", ".name", ".title", "[itemprop='name']"]:
        n = card.select_one(sel)
        if n:
            txt = n.get_text(" ", strip=True)
            if txt:
                return txt
    return link.get_text(" ", strip=True)


def collect_product_cards_from_html(
    html: str,
    page_url: str,
    base_host: str,
    section_path: str,
) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")

    link_candidates: List[Tuple[Tag, Tag, str]] = []
    signature_count: Dict[str, int] = {}

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href or href.startswith("#"):
            continue
        full = urljoin(page_url, href)
        parsed = urlparse(full)
        if parsed.scheme not in {"http", "https"}:
            continue
        if parsed.netloc.lower() != base_host:
            continue
        if not looks_like_product_link(parsed.path, section_path):
            continue

        card = find_best_container_for_link(a)
        if card is None:
            continue

        sig = container_signature(card)
        signature_count[sig] = signature_count.get(sig, 0) + 1
        link_candidates.append((card, a, full))

    if not signature_count:
        return []

    # Пояснение: выбираем самый частый повторяющийся контейнер как селектор карточки.
    best_signature = max(signature_count.items(), key=lambda x: x[1])[0]

    products: List[Dict[str, str]] = []
    for card, a, full in link_candidates:
        if container_signature(card) != best_signature:
            continue

        product_url = normalize_product_url(full)
        name = extract_name_from_card(card, a)
        preview_url = extract_preview_from_card(card, page_url)
        products.append({
            "name": name,
            "product_url": product_url,
            "preview_url": preview_url,
        })

    return products


def discover_pagination_urls(
    html: str,
    page_url: str,
    base_host: str,
    section_path: str,
) -> List[str]:
    # Пояснение: ищем только пагинацию текущей категории, без ссылок на соседние разделы.
    soup = BeautifulSoup(html, "html.parser")
    urls: Set[str] = set()

    for a in soup.select("a[href]"):
        text = a.get_text(" ", strip=True)
        href = (a.get("href") or "").strip()
        if not href:
            continue

        full = urljoin(page_url, href)
        parsed = urlparse(full)
        if parsed.netloc.lower() != base_host:
            continue

        # Пояснение: пагинация должна оставаться в том же section_path.
        if section_path and not parsed.path.lower().startswith(section_path.lower()):
            continue

        query = parse_qs(parsed.query)
        has_page_param = any(k.lower().startswith("pagen_") or k.lower() in {"page", "p"} for k in query.keys())

        parent_classes = " ".join((a.parent.get("class", []) if isinstance(a.parent, Tag) else [])).lower()
        is_digit_link = text.isdigit() and int(text) >= 2 and any(
            key in parent_classes for key in ["pagination", "pager", "nav-pages", "page-navigation"]
        )

        if has_page_param or is_digit_link:
            urls.add(full)

    return sorted(urls)


def scrape_products_from_section(section_url: str, client: HttpClient, max_pages: int = 50) -> List[Dict[str, str]]:
    # Пояснение: проход по категории с поддержкой пагинации и остановками по отсутствию новых товаров.
    first_page = client.get_page(section_url)
    base_parsed = urlparse(first_page["final_url"])
    base_host = base_parsed.netloc.lower()
    section_path = base_parsed.path.rstrip("/").lower()

    append_log(
        f"Загружена страница категории: source={first_page['source_url']} -> final={first_page['final_url']} | status={first_page['status_code']}"
    )

    queue: List[str] = [first_page["final_url"]]
    seen_pages: Set[str] = set()
    seen_products: Set[str] = set()
    all_products: List[Dict[str, str]] = []

    while queue and len(seen_pages) < max_pages:
        page_url = queue.pop(0)
        if page_url in seen_pages:
            continue
        seen_pages.add(page_url)

        page = first_page if page_url == first_page["final_url"] else client.get_page(page_url)
        products = collect_product_cards_from_html(page["html"], page["final_url"], base_host, section_path)

        new_count = 0
        for p in products:
            if p["product_url"] in seen_products:
                continue
            seen_products.add(p["product_url"])
            all_products.append(p)
            new_count += 1

        append_log(
            f"Парсинг страницы каталога: {page['final_url']} | карточек={len(products)} | новых={new_count}"
        )
        if len(products) > 120:
            append_log("WARN: подозрительно много карточек на странице — вероятно, в выборку попали ссылки меню/рекомендаций.")

        # Пояснение: если новых товаров нет, дальнейшая пагинация обычно бессмысленна.
        if len(products) == 0 or new_count == 0:
            continue

        for nxt in discover_pagination_urls(page["html"], page["final_url"], base_host, section_path):
            if nxt not in seen_pages and nxt not in queue:
                queue.append(nxt)

    return all_products


def is_probably_technical_image(url: str, css_classes: str = "", alt_text: str = "") -> bool:
    sample = f"{url} {css_classes} {alt_text}".lower()
    return any(hint in sample for hint in TECH_IMAGE_HINTS)


def _looks_like_800x800(node: Tag, image_url: str) -> bool:
    sample = image_url.lower()
    if "800x800" in sample or "/800/800" in sample or "w=800" in sample:
        return True

    width = str(node.get("width", "")).strip()
    height = str(node.get("height", "")).strip()
    if width == "800" and height == "800":
        return True

    data_w = str(node.get("data-width", "")).strip()
    data_h = str(node.get("data-height", "")).strip()
    return data_w == "800" and data_h == "800"


def _normalize_image_url(image_url: str, base_url: str) -> str:
    # Пояснение: нормализация URL изображения + удаление трекинговых query-параметров.
    abs_url = urljoin(base_url, image_url.strip())
    p = urlparse(abs_url)
    query = parse_qs(p.query)
    keep = {}
    for k, v in query.items():
        lk = k.lower()
        if lk.startswith("utm_") or lk in {"ysclid", "gclid", "fbclid"}:
            continue
        keep[k] = v
    new_query = urlencode([(k, vv) for k, vals in keep.items() for vv in vals])
    return urlunparse((p.scheme, p.netloc, p.path, "", new_query, ""))


def _image_url_score(url: str) -> int:
    # Пояснение: оценка релевантности картинки в fallback-режиме.
    u = url.lower()
    score = 0
    if any(k in u for k in ["product", "catalog", "iblock"]):
        score += 3
    if any(k in u for k in ["800x800", "1200", "1000"]):
        score += 2
    if is_probably_technical_image(u):
        score -= 5
    return score


def extract_main_image_from_product(soup: BeautifulSoup, product_url: str) -> Tuple[str, str]:
    # Приоритет 1: og:image
    og = soup.select_one("meta[property='og:image'][content]")
    if og and og.get("content"):
        return _normalize_image_url(og["content"], product_url), "og:image"

    # Приоритет 2: JSON-LD Product.image
    for script in soup.select("script[type='application/ld+json']"):
        txt = (script.string or script.get_text() or "").strip()
        if not txt:
            continue
        try:
            payload = json.loads(txt)
        except Exception:
            continue

        stack = [payload]
        while stack:
            item = stack.pop()
            if isinstance(item, dict):
                typ = str(item.get("@type", "")).lower()
                image_val = item.get("image")
                if typ == "product" and image_val:
                    if isinstance(image_val, str):
                        return _normalize_image_url(image_val, product_url), "jsonld"
                    if isinstance(image_val, list) and image_val:
                        first = next((x for x in image_val if isinstance(x, str) and x.strip()), "")
                        if first:
                            return _normalize_image_url(first, product_url), "jsonld"
                for v in item.values():
                    stack.append(v)
            elif isinstance(item, list):
                stack.extend(item)

    # Приоритет 3: галерея, с акцентом на класс product-detail-gallery__picture...
    gallery_candidates: List[Tuple[int, str]] = []
    strict_selector = "img.product-detail-gallery__picture.rounded3.zoom_picture.lazyloaded"
    for node in soup.select(strict_selector):
        raw = (
            node.get("data-zoom")
            or node.get("data-large")
            or node.get("data-src")
            or node.get("src")
            or ""
        ).strip()
        if not raw:
            continue
        full = _normalize_image_url(raw, product_url)
        if is_probably_technical_image(full, " ".join(node.get("class", [])), node.get("alt", "")):
            continue
        score = 100 + (20 if _looks_like_800x800(node, full) else 0)
        gallery_candidates.append((score, full))

    gallery_selectors = [
        ".product-detail-gallery a[href]",
        ".product-detail-gallery img",
        ".product-gallery a[href]",
        ".product-gallery img",
        ".swiper img",
    ]
    for sel in gallery_selectors:
        for node in soup.select(sel):
            raw = (
                node.get("data-zoom")
                or node.get("data-large")
                or node.get("data-src")
                or node.get("src")
                or node.get("href")
                or ""
            ).strip()
            if not raw:
                continue
            full = _normalize_image_url(raw, product_url)
            if is_probably_technical_image(full, " ".join(node.get("class", [])), node.get("alt", "")):
                continue
            score = 50 + _image_url_score(full)
            gallery_candidates.append((score, full))

    if gallery_candidates:
        gallery_candidates.sort(key=lambda x: x[0], reverse=True)
        return gallery_candidates[0][1], "gallery"

    # Приоритет 4: fallback по всем изображениям
    fallback: List[Tuple[int, str]] = []
    for img in soup.select("img"):
        raw = (img.get("data-src") or img.get("src") or "").strip()
        if not raw:
            continue
        full = _normalize_image_url(raw, product_url)
        if is_probably_technical_image(full, " ".join(img.get("class", [])), img.get("alt", "")):
            continue

        w = int(str(img.get("width", "0")).strip() or "0") if str(img.get("width", "")).isdigit() else 0
        h = int(str(img.get("height", "0")).strip() or "0") if str(img.get("height", "")).isdigit() else 0
        if (w and w < 200) or (h and h < 200):
            continue

        fallback.append((_image_url_score(full), full))

    if fallback:
        fallback.sort(key=lambda x: x[0], reverse=True)
        return fallback[0][1], "fallback"

    raise ValueError("Не найдено главное изображение товара")


def _extract_six_digit_article(text: str) -> str:
    # Пояснение: извлекаем ровно 6 цифр после маркера "Артикул:".
    if not text:
        return ""
    m = re.search(r"артикул\s*:\s*(\d{6})", text, flags=re.IGNORECASE)
    if m:
        return m.group(1)
    # Пояснение: fallback — первая последовательность из 6 цифр в тексте.
    m2 = re.search(r"\b(\d{6})\b", text)
    return m2.group(1) if m2 else ""


def extract_article(soup: BeautifulSoup) -> str:
    sku_meta = soup.find("meta", attrs={"property": "product:retailer_item_id"}) or soup.find(
        "meta", attrs={"name": "sku"}
    )
    if sku_meta and sku_meta.get("content"):
        direct = _extract_six_digit_article(str(sku_meta.get("content", "")))
        if direct:
            return direct

    for selector in ["[itemprop='sku']", ".sku", "#sku", "[data-sku]", ".product-sku", ".article", "#article"]:
        node = soup.select_one(selector)
        if node:
            val = (node.get("data-sku") or node.get_text(" ", strip=True) or "").strip()
            direct = _extract_six_digit_article(val)
            if direct:
                return direct

    # Пояснение: последний fallback — поиск по всему тексту карточки.
    return _extract_six_digit_article(soup.get_text(" ", strip=True))


def _availability_from_json_ld(soup: BeautifulSoup) -> Optional[bool]:
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
    availability_node = soup.select_one("link[itemprop='availability'], meta[itemprop='availability']")
    if availability_node:
        href = str(availability_node.get("href", "")).lower()
        content = str(availability_node.get("content", "")).lower()
        if "instock" in href or "instock" in content:
            return True
        if "outofstock" in href or "outofstock" in content:
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
    return bool(buy_btn and not buy_btn.has_attr("disabled"))


def extract_product_data(product: Dict[str, str], client: HttpClient) -> Dict[str, str]:
    # Пояснение: полная обработка карточки по ТЗ с указанием источника главного изображения.
    page = client.get_page(product["product_url"])
    soup = BeautifulSoup(page["html"], "html.parser")

    article = extract_article(soup)
    in_stock = is_product_in_stock(soup)
    main_image_url, source = extract_main_image_from_product(soup, page["final_url"])

    head_info = client.head_or_get_image(main_image_url, referer=f"{urlparse(page['final_url']).scheme}://{urlparse(page['final_url']).netloc}")
    if "image" not in head_info.get("content_type", "").lower() and not re.search(r"\.(jpg|jpeg|png|webp|gif|bmp|avif|svg)(\?|$)", main_image_url, re.I):
        append_log(f"WARN: возможно не image URL: {main_image_url} | content-type={head_info.get('content_type', '')}")

    return {
        "product_url": page["final_url"],
        "name": product.get("name", ""),
        "preview_url": product.get("preview_url", ""),
        "article": article,
        "in_stock": "1" if in_stock else "0",
        "main_image_url": main_image_url,
        "main_image_source": source,
    }


def get_second_level_category_name(section_url: str) -> str:
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
    buf = BytesIO()
    with ZipFile(buf, "w", ZIP_DEFLATED) as zf:
        for filename, payload in files_payload:
            zf.writestr(filename, payload)
    return buf.getvalue()


def process_sections(section_urls: List[str], log_placeholder, progress_placeholder, status_placeholder):
    all_report_files: List[Tuple[str, bytes]] = []
    total_checked = 0
    total_in_stock = 0
    total_skipped_not_in_stock = 0
    total_errors = 0
    total_found_cards = 0

    client = HttpClient()
    status_placeholder.info("Ожидание запуска обработки...")
    progress = progress_placeholder.progress(0.0)
    total_sections = len(section_urls)

    for section_idx, section_url in enumerate(section_urls, start=1):
        status_placeholder.info(f"Парсинг раздела {section_idx}/{total_sections}")
        log_step(f"Раздел {section_idx}/{total_sections}: {section_url}", log_placeholder)

        try:
            status_placeholder.info("Парсинг страницы каталога")
            products = scrape_products_from_section(section_url, client)
            total_found_cards += len(products)
            log_step(f"Итого уникальных карточек в категории: {len(products)}", log_placeholder)
        except Exception as exc:
            total_errors += 1
            log_step(f"Ошибка парсинга категории: {exc}", log_placeholder)
            status_placeholder.info(f"Завершен раздел {section_idx}/{total_sections}")
            progress.progress(section_idx / max(1, total_sections))
            continue

        section_rows: List[Dict[str, str]] = []
        for card_idx, product in enumerate(products, start=1):
            if st.session_state.get("stop_requested", False):
                status_placeholder.warning("Получена команда СТОП")
                log_step("Получена команда СТОП. Останавливаем обработку после текущего шага.", log_placeholder)
                break

            status_placeholder.info(f"Обрабатывается карточка {card_idx} из {len(products)}")
            log_step(f"Обрабатывается карточка {card_idx} из {len(products)}", log_placeholder)
            total_checked += 1
            try:
                item = extract_product_data(product, client)
                if item["in_stock"] != "1":
                    total_skipped_not_in_stock += 1
                    log_step(f"Пропущено (нет в наличии): {item['product_url']}", log_placeholder)
                    continue

                total_in_stock += 1
                tineye_url = build_tineye_search_url(item["main_image_url"])
                section_rows.append(
                    {
                        "Артикул": item["article"],
                        "Ссылка на сайт": item["product_url"],
                        "TinEye URL запроса": tineye_url,
                    }
                )
                log_step(
                    f"OK (в наличии): {item['product_url']} | image_source={item['main_image_source']}",
                    log_placeholder,
                )
            except Exception as exc:
                total_errors += 1
                log_step(f"Ошибка карточки {product['product_url']}: {exc}", log_placeholder)


        chunks = split_records(section_rows, REPORT_CHUNK_SIZE)
        category_name = get_second_level_category_name(section_url)
        for i, chunk_rows in enumerate(chunks, start=1):
            file_suffix = f"_{i}" if len(chunks) > 1 else ""
            filename = f"{category_name}{file_suffix}.xlsx"
            all_report_files.append((filename, build_report(chunk_rows)))

        log_step(f"Файлов по разделу: {len(chunks)} | учтены только товары в наличии", log_placeholder)
        status_placeholder.info(f"Завершен раздел {section_idx}/{total_sections}")
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
        "found_cards": total_found_cards,
    }
    return all_report_files, stats



def build_zip_filename(section_urls: List[str]) -> str:
    # Пояснение: имя ZIP формируем от имени категории, как просил пользователь.
    if len(section_urls) == 1:
        return f"{get_second_level_category_name(section_urls[0])}.zip"
    if section_urls:
        return f"{get_second_level_category_name(section_urls[0])}_multi.zip"
    return "report.zip"

def main() -> None:
    st.set_page_config(page_title="TinEye URL-отчеты по товарам", layout="wide")
    st.title("Генерация TinEye URL по товарам в наличии")

    st.markdown(
        """
<style>
.log-box {
  height: 190px;
  overflow-y: auto;
  border: 1px solid #d9d9d9;
  border-radius: 8px;
  padding: 8px 10px;
  background: #0e1117;
}
.log-box .log-lines {
  margin: 0;
  white-space: normal;
  word-break: break-word;
  color: #f0f2f6;
  font-size: 12px;
  line-height: 1.35;
}
</style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
- На вход подается ссылка на раздел сайта (или XLS/XLSX со ссылками разделов).
- Выполняется парсинг категории с дедупликацией карточек и учетом пагинации.
- В отчет попадают только товары в наличии.
        """
    )

    st.session_state.setdefault("stop_requested", False)

    input_mode = st.radio("Способ ввода", ["Ручной ввод URL разделов", "Загрузка XLS"], horizontal=True)
    source_df = None
    mapping_confirmed: Dict[str, str] = {}

    if input_mode == "Ручной ввод URL разделов":
        # Пояснение: поле ручного ввода сделано в одну строку по требованию.
        one_url = st.text_input("Введите ссылку на раздел сайта")
        source_df = make_rows_from_manual_input(one_url)
        mapping_confirmed = {"input_url": "input_url"}
    else:
        upload = st.file_uploader("Загрузите XLS/XLSX", type=["xls", "xlsx"])
        if upload is not None:
            df_raw = read_excel(upload)

            # Пояснение: по требованию после загрузки оставляем только колонку со ссылками.
            link_col = None
            for col in df_raw.columns:
                if normalize_text(str(col)) == normalize_text("Ссылка"):
                    link_col = col
                    break

            if link_col is None:
                auto_mapping, _ = auto_map_columns(df_raw.columns)
                link_col = auto_mapping.get("input_url")

            if not link_col:
                st.error("Не найдена колонка 'Ссылка' (или эквивалент с URL).")
            else:
                df_links = df_raw[[link_col]].copy()
                df_links.columns = ["Ссылка"]
                st.subheader("Данные после очистки (оставлена только колонка 'Ссылка')")
                st.dataframe(df_links.head(20), use_container_width=True)

                mapping_confirmed = {"input_url": "Ссылка"}
                source_df = make_rows_from_excel(df_links, mapping_confirmed)

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

    files_payload: List[Tuple[str, bytes]] = []
    stats = None

    # Пояснение: над полосой прогресса показываем текущий статус обработки.
    st.subheader("Статус обработки")
    status_placeholder = st.empty()
    status_placeholder.info("Ожидание запуска")

    # Пояснение: полоса прогресса размещена над логами по требованию.
    st.subheader("Прогресс обработки")
    progress_placeholder = st.empty()

    # Пояснение: плейсхолдер логов создаем до запуска, чтобы обновлять логи в реальном времени.
    with st.expander("Логи обработки", expanded=False):
        bottom_log_placeholder = st.empty()
        render_logs(bottom_log_placeholder)

    if run:
        st.session_state["ui_logs"] = []
        st.session_state["stop_requested"] = False
        log_step("Старт обработки", bottom_log_placeholder)

        if not section_urls:
            status_placeholder.error("Нет ссылок разделов для обработки")
            st.warning("Нет ссылок разделов для обработки.")
            return

        files_payload, stats = process_sections(section_urls, bottom_log_placeholder, progress_placeholder, status_placeholder)

    if stats is not None:
        status_placeholder.success("Обработка завершена")
        st.success(
            "Готово. "
            f"Карточек найдено: {stats['found_cards']} | "
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
                file_name=build_zip_filename(section_urls),
                mime="application/zip",
            )
            with st.expander("Список сформированных файлов", expanded=False):
                for filename, _ in files_payload:
                    st.write(filename)


if __name__ == "__main__":
    main()
