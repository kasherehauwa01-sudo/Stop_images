from __future__ import annotations

import re
from typing import Dict, Iterable, Optional, Tuple

# Пояснение: на входе теперь ссылка на раздел/страницу каталога,
# поэтому обязательной является только колонка с URL раздела сайта.
REQUIRED_FIELDS_SYNONYMS = {
    "input_url": [
        "ссылка",
        "ссылка на раздел",
        "ссылка на сайт",
        "url",
        "link",
        "section_url",
        "catalog_url",
        "input_url",
    ],
}

FIELD_LABELS = {
    "input_url": "Ссылка на раздел сайта",
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
