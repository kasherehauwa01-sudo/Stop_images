from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List
from urllib.parse import urljoin, urlparse

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

    def search_by_url(self, image_url: str, top_n: int = 20) -> List[Dict[str, str]]:
        # Пояснение: режим со скрапингом — забираем HTML страницы результатов TinEye
        # и парсим ссылки-источники совпадений.
        search_url = self.settings.base_url.rstrip("/") + "/search"
        response = self.session.get(
            search_url,
            params={"url": image_url},
            timeout=self.settings.timeout_seconds,
            allow_redirects=True,
        )
        response.raise_for_status()
        return self._parse_results(response.text, response.url, top_n=top_n)

    def _parse_results(self, html: str, final_url: str, top_n: int) -> List[Dict[str, str]]:
        soup = BeautifulSoup(html, "html.parser")
        normalized: List[Dict[str, str]] = []

        # Пояснение: на странице встречаются разные шаблоны блоков результатов,
        # поэтому выбираем несколько CSS-селекторов и собираем уникальные URL.
        selectors = [
            "div.match a[href]",
            "div.results a[href]",
            "section.results a[href]",
            "ul.matches a[href]",
            "a.match-link[href]",
        ]

        candidates = []
        for selector in selectors:
            candidates.extend(soup.select(selector))

        seen = set()
        for a in candidates:
            href = (a.get("href") or "").strip()
            if not href:
                continue
            abs_url = urljoin(final_url, href)
            if abs_url in seen:
                continue
            seen.add(abs_url)

            host = urlparse(abs_url).netloc.lower()
            title = (a.get_text(" ", strip=True) or "")[:500]
            normalized.append({"page_url": abs_url, "domain": host, "title": title})
            if len(normalized) >= top_n:
                break

        return normalized
