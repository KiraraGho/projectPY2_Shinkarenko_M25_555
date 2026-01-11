from __future__ import annotations

from typing import Any

# Поддерживаемые типы данных столбцов
SUPPORTED_TYPES = {"int", "str", "bool"}


def _format_columns_for_print(columns: list[dict[str, str]]) -> str:
    """
    Формирует строку для красивого вывода списка столбцов.
    Пример: ID:int, name:str, age:int
    """
    return ", ".join(f"{col['name']}:{col['type']}" for col in columns)


def create_table(
    metadata: dict[str, Any],
    table_name: str,
    columns: list[str],
) -> dict[str, Any]:
    """
    Создаёт таблицу с заданным именем и столбцами.

    - Проверяет, существует ли таблица
    - Проверяет корректность типов данных
    - Автоматически добавляет столбец ID:int (если он не указан)
    """
    metadata.setdefault("tables", {})
    tables: dict[str, Any] = metadata["tables"]

    # Проверка на существование таблицы
    if table_name in tables:
        print(f'Ошибка: Таблица "{table_name}" уже существует.')
        return metadata

    parsed_columns: list[dict[str, str]] = []

    # Разбор столбцов формата <имя:тип>
    for raw_column in columns:
        if ":" not in raw_column:
            print(f"Некорректное значение: {raw_column}. Попробуйте снова.")
            return metadata

        name, col_type = raw_column.split(":", 1)
        name = name.strip()
        col_type = col_type.strip()

        if not name or not col_type:
            print(f"Некорректное значение: {raw_column}. Попробуйте снова.")
            return metadata

        if col_type not in SUPPORTED_TYPES:
            print(f"Некорректное значение: {col_type}. Попробуйте снова.")
            return metadata

        parsed_columns.append({"name": name, "type": col_type})

    # Проверяем наличие ID
    has_id = any(col["name"] == "ID" for col in parsed_columns)

    if not has_id:
        # Если ID не задан — добавляем автоматически
        parsed_columns.insert(0, {"name": "ID", "type": "int"})
    else:
        # Если ID задан — проверяем, что его тип int
        for col in parsed_columns:
            if col["name"] == "ID" and col["type"] != "int":
                print("Некорректное значение: ID должен иметь тип int."
                      "Попробуйте снова.")
                return metadata

        # Перемещаем ID в начало списка
        parsed_columns = (
            [col for col in parsed_columns if col["name"] == "ID"]
            + [col for col in parsed_columns if col["name"] != "ID"]
        )

    # Сохраняем таблицу в метаданные
    tables[table_name] = {"columns": parsed_columns}

    print(
        f'Таблица "{table_name}" успешно создана со столбцами: '
        f"{_format_columns_for_print(parsed_columns)}"
    )
    return metadata


def drop_table(metadata: dict[str, Any], table_name: str) -> dict[str, Any]:
    """
    Удаляет таблицу из метаданных.
    """
    tables: dict[str, Any] = metadata.get("tables", {})

    if table_name not in tables:
        print(f'Ошибка: Таблица "{table_name}" не существует.')
        return metadata

    del tables[table_name]
    metadata["tables"] = tables

    print(f'Таблица "{table_name}" успешно удалена.')
    return metadata


def list_tables(metadata: dict[str, Any]) -> None:
    """
    Выводит список всех существующих таблиц.
    """
    tables: dict[str, Any] = metadata.get("tables", {})

    if not tables:
        print("(таблиц нет)")
        return

    for table_name in sorted(tables.keys()):
        print(f"- {table_name}")
