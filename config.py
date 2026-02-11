from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import streamlit as st


@dataclass
class StockRule:
    source_type: str
    host: str
    path_prefix: str


def load_stock_rules(config_path: Path) -> List[StockRule]:
    # Пояснение: список стоков хранится в json-конфиге, чтобы менять домены без правок кода.
    with config_path.open("r", encoding="utf-8") as f:
        payload: Dict[str, List[str]] = json.load(f)

    rules: List[StockRule] = []
    for source_type, entries in payload.items():
        for raw in entries:
            item = raw.strip().lower()
            if not item:
                continue
            if "/" in item:
                host, path_prefix = item.split("/", 1)
                path_prefix = "/" + path_prefix.strip("/")
            else:
                host, path_prefix = item, ""
            rules.append(StockRule(source_type=source_type, host=host, path_prefix=path_prefix))
    return rules


def classify_stock_url(candidate_url: str, rules: List[StockRule]) -> Optional[tuple[str, str]]:
    from urllib.parse import urlparse

    parsed = urlparse(candidate_url)
    host = parsed.netloc.lower()
    path = parsed.path.rstrip("/")

    for rule in rules:
        if host == rule.host or host.endswith(f".{rule.host}"):
            if rule.path_prefix and not path.startswith(rule.path_prefix):
                continue
            return candidate_url, rule.source_type
    return None


def read_secret_or_env(key: str, default: str = "") -> str:
    # Пояснение: для Streamlit Cloud удобно брать данные из st.secrets,
    # а для локального запуска — из переменных окружения.
    import os

    value = st.secrets.get(key, "") if hasattr(st, "secrets") else ""
    if value:
        return str(value)
    return os.getenv(key, default)
