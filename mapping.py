from __future__ import annotations

import re
from typing import Dict, Iterable, Optional, Tuple

# Пояснение: в текущем сценарии обязательным считаем только URL страницы-источника,
# где размещено изображение и откуда нужно извлечь артикул товара.
REQUIRED_FIELDS_SYNONYMS = {
    "image_url": [
        "ссылка",
        "ссылка на сайт",
        "ссылка на страницу",
        "url",
        "link",
        "page_url",
        "product_url",
        "image_url",
    ],
}

FIELD_LABELS = {
    "image_url": "Ссылка на страницу сайта",
}


def normalize_text(value: str) -> str:
    cleaned = str(value).strip().lower().replace("ё", "е")
    return re.sub(r"\s+", "", cleaned)


def auto_map_columns(columns: Iterable[str]) -> Tuple[Dict[str, Optional[str]], bool]:
    normalized_columns = {col: normalize_text(col) for col in columns}
    mapping: Dict[str, Optional[str]] = {}
    needs_manual = False

    for field, synonyms in REQUIRED_FIELDS_SYNONYMS.items():
        normalized_synonyms = {normalize_text(s) for s in synonyms}
        matched = [col for col, val in normalized_columns.items() if val in normalized_synonyms]
        if len(matched) == 1:
            mapping[field] = matched[0]
        else:
            mapping[field] = None
            needs_manual = True

    return mapping, needs_manual
