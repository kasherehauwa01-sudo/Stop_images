from __future__ import annotations

import re
from typing import Dict, Iterable, Optional, Tuple

# Пояснение: набор синонимов для гибкого автоматического сопоставления колонок XLS.
REQUIRED_FIELDS_SYNONYMS = {
    "image_code": ["код изображения", "image_code", "code", "id", "артикул", "sku"],
    "image_url": ["ссылка на изображение", "image_url", "url", "link", "image", "фото"],
    "product_name": ["наименование товара", "товар", "product", "product_name", "name"],
    "supplier": ["поставщик", "supplier", "vendor"],
    "manager": ["менеджер", "manager", "ответственный"],
}

FIELD_LABELS = {
    "image_code": "Код изображения",
    "image_url": "Ссылка на изображение (URL)",
    "product_name": "Наименование товара",
    "supplier": "Поставщик",
    "manager": "Менеджер",
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
