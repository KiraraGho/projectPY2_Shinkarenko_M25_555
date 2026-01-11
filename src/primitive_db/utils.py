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

def load_table_data(table_name: str) -> list[dict[str, Any]]:
    """
    Загружает данные конкретной таблицы из data/<table_name>.json.

    Если файла нет — возвращает пустой список (таблица пока без записей).
    """
    filepath = f"data/{table_name}.json"
    try:
        with open(filepath, "r", encoding="utf-8") as file:
            return json.load(file)
    except FileNotFoundError:
        return []


def save_table_data(table_name: str, data: list[dict[str, Any]]) -> None:
    """
    Сохраняет данные конкретной таблицы в data/<table_name>.json.
    """
    Path("data").mkdir(parents=True, exist_ok=True)
    filepath = f"data/{table_name}.json"
    with open(filepath, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
