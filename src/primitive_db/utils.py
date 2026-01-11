from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_metadata(filepath: str) -> dict[str, Any]:
    """
    Загружает метаданные базы данных из JSON-файла.
    Если файл не найден, возвращает пустой словарь.
    """
    try:
        with open(filepath, "r", encoding="utf-8") as file:
            return json.load(file)
    except FileNotFoundError:
        return {}


def save_metadata(filepath: str, data: dict[str, Any]) -> None:
    """
    Сохраняет метаданные базы данных в JSON-файл.
    """
    path = Path(filepath)
    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
