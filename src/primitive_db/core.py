from __future__ import annotations

from typing import Any

from src.decorators import confirm_action, handle_db_errors, log_time

# Поддерживаемые типы данных столбцов
SUPPORTED_TYPES = {"int", "str", "bool"}


def _format_columns_for_print(columns: list[dict[str, str]]) -> str:
    """Формирует строку для красивого вывода списка столбцов."""
    return ", ".join(f"{col['name']}:{col['type']}" for col in columns)


def _get_tables(metadata: dict[str, Any]) -> dict[str, Any]:
    """
    Возвращает словарь таблиц из metadata.
    Если секции нет — создаёт её.
    """
    metadata.setdefault("tables", {})
    tables: dict[str, Any] = metadata["tables"]
    return tables


def _get_table_schema(
    metadata: dict[str, Any],
    table_name: str,
) -> list[dict[str, str]]:
    """
    Возвращает схему таблицы (список столбцов).

    Если таблицы нет — бросает KeyError, который обработает декоратор.
    """
    tables = _get_tables(metadata)
    if table_name not in tables:
        raise KeyError(table_name)

    schema = tables[table_name].get("columns")
    if not isinstance(schema, list):
        raise ValueError("Схема таблицы повреждена.")
    return schema


def _get_col_type(schema: list[dict[str, str]], col_name: str) -> str:
    """Возвращает тип столбца по имени или бросает KeyError."""
    for col in schema:
        if col.get("name") == col_name:
            col_type = col.get("type")
            if col_type in SUPPORTED_TYPES:
                return col_type
            raise ValueError(f"Некорректный тип столбца в схеме: {col_type}")
    raise KeyError(col_name)


def _convert_value(raw: str, expected_type: str) -> Any:
    """
    Приводит строковое значение к типу из схемы.

    Важно: по условию упрощения строки должны быть в кавычках:
      "Sergei" или 'Sergei'
    """
    if expected_type == "str":
        if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in {'"', "'"}:
            return raw[1:-1]
        raise ValueError(
            f'Строковое значение должно быть в кавычках: {raw}',
        )

    if expected_type == "int":
        try:
            return int(raw)
        except ValueError as exc:
            raise ValueError(f"Ожидалось целое число: {raw}") from exc

    if expected_type == "bool":
        low = raw.lower()
        if low in {"true", "false"}:
            return low == "true"
        raise ValueError(f"Ожидалось булево true/false: {raw}")

    raise ValueError(f"Неподдерживаемый тип: {expected_type}")


@handle_db_errors
def create_table(
    metadata: dict[str, Any],
    table_name: str,
    columns: list[str],
) -> dict[str, Any]:
    """
    Создаёт таблицу:
    - Проверяет, что таблицы ещё нет
    - Валидирует типы столбцов
    - Добавляет ID:int автоматически (если пользователь не передал ID)
    """
    tables = _get_tables(metadata)

    if table_name in tables:
        raise ValueError(f'Таблица "{table_name}" уже существует.')

    parsed_columns: list[dict[str, str]] = []

    # Разбор колонок формата <имя:тип>
    for raw_column in columns:
        if ":" not in raw_column:
            raise ValueError(f"Некорректное значение: {raw_column}")

        name, col_type = raw_column.split(":", 1)
        name = name.strip()
        col_type = col_type.strip()

        if not name or not col_type:
            raise ValueError(f"Некорректное значение: {raw_column}")

        if col_type not in SUPPORTED_TYPES:
            raise ValueError(f"Некорректное значение: {col_type}")

        parsed_columns.append({"name": name, "type": col_type})

    # Проверяем наличие ID
    has_id = any(col["name"] == "ID" for col in parsed_columns)

    if not has_id:
        parsed_columns.insert(0, {"name": "ID", "type": "int"})
    else:
        # Проверяем, что ID:int
        for col in parsed_columns:
            if col["name"] == "ID" and col["type"] != "int":
                raise ValueError("ID должен иметь тип int")

        # Перемещаем ID в начало
        parsed_columns = (
            [col for col in parsed_columns if col["name"] == "ID"]
            + [col for col in parsed_columns if col["name"] != "ID"]
        )

    tables[table_name] = {"columns": parsed_columns}

    print(
        f'Таблица "{table_name}" успешно создана со столбцами: '
        f"{_format_columns_for_print(parsed_columns)}"
    )
    return metadata


@handle_db_errors
@confirm_action("удаление таблицы")
def drop_table(metadata: dict[str, Any], table_name: str) -> dict[str, Any]:
    """Удаляет таблицу из метаданных."""
    tables = _get_tables(metadata)

    if table_name not in tables:
        raise KeyError(table_name)

    del tables[table_name]
    print(f'Таблица "{table_name}" успешно удалена.')
    return metadata


@handle_db_errors
def list_tables(metadata: dict[str, Any]) -> None:
    """Печатает список таблиц."""
    tables = _get_tables(metadata)

    if not tables:
        print("(таблиц нет)")
        return

    for name in sorted(tables.keys()):
        print(f"- {name}")


@handle_db_errors
@log_time
def insert(
    metadata: dict[str, Any],
    table_name: str,
    table_data: list[dict[str, Any]],
    values: list[str],
) -> tuple[list[dict[str, Any]], int]:
    """
    INSERT:
    - Проверяет существование таблицы
    - Проверяет количество значений (без ID)
    - Валидирует типы значений по схеме
    - Генерирует новый ID (max(ID)+1)
    - Добавляет запись и возвращает (данные, новый_id)
    """
    schema = _get_table_schema(metadata, table_name)

    # Пользователь не передаёт ID
    user_columns = [c for c in schema if c["name"] != "ID"]

    if len(values) != len(user_columns):
        raise ValueError("Количество значений не соответствует схеме.")

    record: dict[str, Any] = {}

    # Конвертация по схеме
    for col, raw_val in zip(user_columns, values, strict=True):
        col_name = col["name"]
        col_type = col["type"]
        record[col_name] = _convert_value(raw_val, col_type)

    # Генерация ID
    existing_ids = [
        row.get("ID", 0)
        for row in table_data
        if isinstance(row.get("ID"), int)
    ]
    new_id = (max(existing_ids) + 1) if existing_ids else 1
    record["ID"] = new_id

    table_data.append(record)
    return table_data, new_id


@handle_db_errors
@log_time
def select(
    table_data: list[dict[str, Any]],
    where_clause: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """
    SELECT:
    - Без where возвращает все записи
    - С where фильтрует записи по точному совпадению
    """
    if where_clause is None:
        return table_data

    key, value = next(iter(where_clause.items()))
    return [row for row in table_data if row.get(key) == value]


@handle_db_errors
def update(
    table_data: list[dict[str, Any]],
    set_clause: dict[str, Any],
    where_clause: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[int]]:
    """
    UPDATE:
    - Находит записи по where_clause
    - Обновляет значения по set_clause
    - Возвращает (новые_данные, список ID обновлённых записей)
    """
    where_key, where_val = next(iter(where_clause.items()))
    updated_ids: list[int] = []

    for row in table_data:
        if row.get(where_key) == where_val:
            for key, value in set_clause.items():
                row[key] = value
            if isinstance(row.get("ID"), int):
                updated_ids.append(row["ID"])

    return table_data, updated_ids


@handle_db_errors
@confirm_action("удаление записей")
def delete(
    table_data: list[dict[str, Any]],
    where_clause: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[int]]:
    """
    DELETE:
    - Удаляет записи по where_clause
    - Возвращает (новые_данные, список ID удалённых записей)
    """
    where_key, where_val = next(iter(where_clause.items()))
    kept: list[dict[str, Any]] = []
    deleted_ids: list[int] = []

    for row in table_data:
        if row.get(where_key) == where_val:
            if isinstance(row.get("ID"), int):
                deleted_ids.append(row["ID"])
        else:
            kept.append(row)

    return kept, deleted_ids


@handle_db_errors
def table_info(
    metadata: dict[str, Any],
    table_name: str,
    table_data: list[dict[str, Any]],
) -> None:
    """Печатает информацию о таблице: схема и количество записей."""
    schema = _get_table_schema(metadata, table_name)
    cols = _format_columns_for_print(schema)

    print(f"Таблица: {table_name}")
    print(f"Столбцы: {cols}")
    print(f"Количество записей: {len(table_data)}")
