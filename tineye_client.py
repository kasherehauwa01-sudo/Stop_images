from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, List, Optional
from urllib.parse import parse_qs, unquote, urljoin, urlparse, urlencode

import requests
from bs4 import BeautifulSoup


@dataclass
class TinEyeScraperSettings:
    base_url: str
    timeout_seconds: int = 30


class TinEyeScraperClient:
    def __init__(self, settings: TinEyeScraperSettings):
        self.settings = settings
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            }
        )

    def is_configured(self) -> bool:
        return bool(self.settings.base_url)

    def build_search_url(self, image_url: str) -> str:
        return f"{self.settings.base_url.rstrip('/')}/search?" + urlencode({"url": image_url})

    def search_by_url(self, image_url: str, top_n: int = 20) -> List[Dict[str, str]]:
        # Пояснение: строим URL для поля "Search by image url" и парсим именно эту страницу результатов.
        request_url = self.build_search_url(image_url)
        return self.search_by_tineye_url(request_url, top_n=top_n)

    def search_by_tineye_url(self, request_url: str, top_n: int = 20) -> List[Dict[str, str]]:
        # Пояснение: отдельный метод для явного парсинга уже сформированного TinEye URL из UI.
        response = self.session.get(
            request_url,
            timeout=self.settings.timeout_seconds,
            allow_redirects=True,
        )
        response.raise_for_status()
        return self._parse_results(response.text, response.url, top_n=top_n)

    def _extract_external_url(self, raw_href: str, final_url: str) -> Optional[str]:
        if not raw_href:
            return None
        abs_url = urljoin(final_url, raw_href.strip())
        parsed = urlparse(abs_url)

        if parsed.netloc and "tineye.com" not in parsed.netloc.lower():
            return abs_url

        query = parse_qs(parsed.query)
        for key in ("url", "u", "target"):
            if key in query and query[key]:
                candidate = unquote(query[key][0])
                c_parsed = urlparse(candidate)
                if c_parsed.scheme in {"http", "https"}:
                    return candidate

        js_match = re.search(r"https?://[^'\"\s)]+", raw_href)
        if js_match:
            return js_match.group(0)

        return None

    def _extract_domains_from_text(self, text: str) -> List[str]:
        if not text:
            return []
        matches = re.findall(r"\b([a-z0-9-]+(?:\.[a-z0-9-]+)+)\b", text.lower())
        out: List[str] = []
        seen = set()
        for domain in matches:
            domain = domain.strip(".")
            if domain.endswith("tineye.com"):
                continue
            if domain not in seen:
                seen.add(domain)
                out.append(domain)
        return out

    def _parse_results(self, html: str, final_url: str, top_n: int) -> List[Dict[str, str]]:
        soup = BeautifulSoup(html, "html.parser")
        normalized: List[Dict[str, str]] = []
        seen = set()

        def push(url: str, title: str = "") -> None:
            if not url or url in seen:
                return
            seen.add(url)
            host = urlparse(url).netloc.lower()
            if not host or host.endswith("tineye.com"):
                return
            normalized.append({"page_url": url, "domain": host, "title": (title or "")[:500]})

        selectors = [
            "div.match a[href]",
            "div.results a[href]",
            "section.results a[href]",
            "ul.matches a[href]",
            "a.match-link[href]",
            "[data-href]",
            "a[href]",
        ]

        # 1) Явные ссылки/атрибуты
        for selector in selectors:
            for node in soup.select(selector):
                href = (node.get("href") or node.get("data-href") or "").strip()
                ext_url = self._extract_external_url(href, final_url)
                if ext_url:
                    push(ext_url, node.get_text(" ", strip=True))
                # Доп. извлечение из dataset-атрибутов
                for attr in ("data-url", "data-link", "data-target", "data-domain"):
                    value = (node.get(attr) or "").strip()
                    if value.startswith("http"):
                        push(value, node.get_text(" ", strip=True))
                    elif value and "." in value and " " not in value:
                        push(f"https://{value}", node.get_text(" ", strip=True))
                if len(normalized) >= top_n:
                    return normalized[:top_n]

        # 2) URL в скриптах
        for script in soup.find_all("script"):
            text = script.string or script.get_text() or ""
            for url in re.findall(r"https?://[^'\"\s<>()]+", text):
                if "tineye.com" in url.lower():
                    continue
                push(url)
                if len(normalized) >= top_n:
                    return normalized[:top_n]

        # 3) Домены в тексте карточек
        card_selectors = [".match", ".result", "li", "article", "[class*='result']", "body"]
        for selector in card_selectors:
            for node in soup.select(selector):
                text = node.get_text(" ", strip=True)
                for domain in self._extract_domains_from_text(text):
                    push(f"https://{domain}", text)
                    if len(normalized) >= top_n:
                        return normalized[:top_n]

        return normalized[:top_n]
