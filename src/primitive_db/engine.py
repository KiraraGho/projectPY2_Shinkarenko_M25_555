from __future__ import annotations

import shlex
from typing import Any

import prompt
from prettytable import PrettyTable

from src.decorators import create_cacher
from src.primitive_db.core import (
    create_table,
    delete,
    drop_table,
    insert,
    list_tables,
    select,
    table_info,
    update,
)
from src.primitive_db.utils import (
    load_metadata,
    load_table_data,
    save_metadata,
    save_table_data,
)

# Файл, где хранятся метаданные (таблицы + схемы столбцов)
META_FILEPATH = "db_meta.json"


def print_help() -> None:
    """Печатает справку по командам базы данных."""
    print("\n***Операции с данными***\n")
    print("Функции:")
    print(
        "<command> insert into <имя_таблицы> values (<значение1>, <значение2>, ...) "
        "- создать запись."
    )
    print(
        "<command> select from <имя_таблицы> where <столбец> = <значение> "
        "- прочитать записи по условию."
    )
    print("<command> select from <имя_таблицы> - прочитать все записи.")
    print(
        "<command> update <имя_таблицы> set <столбец> = <новое_значение> "
        "where <столбец> = <значение> - обновить записи."
    )
    print(
        "<command> delete from <имя_таблицы> where <столбец> = <значение> "
        "- удалить записи."
    )
    print("<command> info <имя_таблицы> - вывести информацию о таблице.\n")

    print("Управление таблицами:")
    print(
        "<command> create_table <имя_таблицы> <столбец1:тип> <столбец2:тип> .. "
        "- создать таблицу"
    )
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу\n")

    print("Общие команды:")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация\n")


def _get_schema(
        metadata: dict[str, Any],
        table_name: str,
        ) -> list[dict[str, str]] | None:
    """Достаёт схему таблицы из метаданных. Если таблицы нет — None."""
    tables = metadata.get("tables", {})
    table = tables.get(table_name)
    if not table:
        return None
    schema = table.get("columns")
    if not isinstance(schema, list):
        return None
    return schema


def _get_col_type(schema: list[dict[str, str]], col_name: str) -> str | None:
    """Возвращает тип столбца по имени (int/str/bool), либо None."""
    for col in schema:
        if col.get("name") == col_name:
            col_type = col.get("type")
            if isinstance(col_type, str):
                return col_type
    return None


def _convert_value(raw: str, expected_type: str) -> object | None:
    """
    Приводит строковое значение к типу.
    Упрощение по ТЗ: строковые значения всегда в кавычках.
    """
    if expected_type == "str":
        if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in {'"', "'"}:
            return raw[1:-1]
        return None

    if expected_type == "int":
        try:
            return int(raw)
        except ValueError:
            return None

    if expected_type == "bool":
        low = raw.lower()
        if low in {"true", "false"}:
            return low == "true"
        return None

    return None


def _parse_values_list(values_part: str) -> list[str] | None:
    s = values_part.strip()
    if not (s.startswith("(") and s.endswith(")")):
        return None

    inner = s[1:-1].strip()
    if inner == "":
        return []

    result: list[str] = []
    buf: list[str] = []
    in_quotes: str | None = None

    for ch in inner:
        if ch in {"'", '"'}:
            if in_quotes is None:
                in_quotes = ch
            elif in_quotes == ch:
                in_quotes = None
            buf.append(ch)
            continue

        # Запятая — разделитель только вне кавычек
        if ch == "," and in_quotes is None:
            token = "".join(buf).strip()
            if token == "":
                return None
            result.append(token)
            buf = []
            continue

        buf.append(ch)

    token = "".join(buf).strip()
    if token == "":
        return None
    result.append(token)

    return result

def _extract_values_part(user_input: str) -> str | None:
    """
    Достаёт подстроку '(<...>)' после ключевого слова values из исходной команды.
    Это нужно, чтобы shlex не срезал кавычки у строк.
    """
    low = user_input.lower()
    idx = low.find(" values ")
    if idx == -1:
        return None

    after = user_input[idx + len(" values ") :].strip()
    # after должен начинаться с "(...)" и содержать закрывающую скобку
    if not after.startswith("("):
        return None
    return after


def _parse_assignment(tokens: list[str]) -> tuple[str, str] | None:
    if not tokens:
        return None

    if len(tokens) == 1 and "=" in tokens[0]:
        left, right = tokens[0].split("=", 1)
        left = left.strip()
        right = right.strip()
        if not left or not right:
            return None
        return left, right

    if len(tokens) >= 3 and tokens[1] == "=":
        left = tokens[0].strip()
        right = tokens[2].strip()
        if not left or not right:
            return None
        return left, right

    return None


def _print_rows(schema: list[dict[str, str]], rows: list[dict[str, Any]]) -> None:
    """Печатает результат SELECT в виде таблицы PrettyTable."""
    if not rows:
        print("(записей нет)")
        return

    headers = [c["name"] for c in schema]
    table = PrettyTable()
    table.field_names = headers

    for row in rows:
        table.add_row([row.get(h) for h in headers])

    print(table)


def run() -> None:
    """
    Главная функция запуска БД.
    Здесь основной цикл: читаем команду -> парсим -> выполняем.
    """
    print("***Операции с данными***")
    print_help()

    # Кэш для select (замыкание). После изменений данных кэш сбрасываем.
    cache_result = create_cacher()

    while True:
        user_input = prompt.string(">>> Введите команду: ").strip()
        if not user_input:
            continue

        try:
            args = shlex.split(user_input)
        except ValueError:
            print(f"Некорректное значение: {user_input}. Попробуйте снова.")
            continue

        command = args[0]
        rest = args[1:]

        # Общие команды
        if command == "exit":
            break

        if command == "help":
            print_help()
            continue

        # Всегда загружаем актуальные метаданные перед операцией
        metadata = load_metadata(META_FILEPATH)

        # Управление таблицами
        if command == "create_table":
            if len(rest) < 2:
                print(
                    "Некорректное значение: недостаточно аргументов. "
                    "Попробуйте снова."
                )
                continue

            table_name = rest[0]
            columns = rest[1:]
            new_metadata = create_table(metadata, table_name, columns)
            if new_metadata is not None:
                save_metadata(META_FILEPATH, new_metadata)
            continue

        if command == "drop_table":
            if len(rest) != 1:
                print("Некорректное значение: ожидается имя таблицы. Попробуйте снова.")
                continue

            table_name = rest[0]
            new_metadata = drop_table(metadata, table_name)
            if new_metadata is not None:
                save_metadata(META_FILEPATH, new_metadata)
            continue

        if command == "list_tables":
            if rest:
                print("Некорректное значение: лишние аргументы. Попробуйте снова.")
                continue
            list_tables(metadata)
            continue

        # INFO
        if command == "info":
            if len(rest) != 1:
                print("Некорректное значение: ожидается имя таблицы. Попробуйте снова.")
                continue

            table_name = rest[0]
            schema = _get_schema(metadata, table_name)
            if schema is None:
                print(f'Ошибка: Таблица "{table_name}" не существует.')
                continue

            table_data = load_table_data(table_name)
            table_info(metadata, table_name, table_data)
            continue

        # INSERT
        if command == "insert":
            if len(rest) < 4 or rest[0] != "into" or rest[2] != "values":
                print(f"Некорректное значение: {user_input}. Попробуйте снова.")
                continue

            table_name = rest[1]
            schema = _get_schema(metadata, table_name)
            if schema is None:
                print(f'Ошибка: Таблица "{table_name}" не существует.')
                continue

            values_part = _extract_values_part(user_input)
            if values_part is None:
                print(f"Некорректное значение: {user_input}. Попробуйте снова.")
                continue

            values = _parse_values_list(values_part)
            if values is None:
                print(f"Некорректное значение: {values_part}. Попробуйте снова.")
                continue


            table_data = load_table_data(table_name)
            result = insert(metadata, table_name, table_data, values)
            if result is None:
                continue

            new_data, new_id = result
            save_table_data(table_name, new_data)

            # Данные изменились — кэш select надо сбросить
            cache_result = create_cacher()

            print(f'Запись с ID={new_id} успешно добавлена в таблицу "{table_name}".')
            continue

        # SELECT
        if command == "select":
            if len(rest) < 2 or rest[0] != "from":
                print(f"Некорректное значение: {user_input}. Попробуйте снова.")
                continue

            table_name = rest[1]
            schema = _get_schema(metadata, table_name)
            if schema is None:
                print(f'Ошибка: Таблица "{table_name}" не существует.')
                continue

            table_data = load_table_data(table_name)

            # SELECT без where
            if len(rest) == 2:
                cache_key = f"select:{table_name}:all"
                rows = cache_result(
                    cache_key,
                    lambda: select(table_data, where_clause=None),
                )
                _print_rows(schema, rows)
                continue

            # SELECT с where
            if len(rest) >= 3 and rest[2] == "where":
                assignment = _parse_assignment(rest[3:])
                if assignment is None:
                    print(f"Некорректное значение: {user_input}. Попробуйте снова.")
                    continue

                col, raw_val = assignment
                col_type = _get_col_type(schema, col)
                if col_type is None:
                    print(f"Некорректное значение: {col}. Попробуйте снова.")
                    continue

                typed_val = _convert_value(raw_val, col_type)
                if typed_val is None:
                    print(f"Некорректное значение: {raw_val}. Попробуйте снова.")
                    continue

                where_clause = {col: typed_val}
                cache_key = f"select:{table_name}:{col}={typed_val!r}"
                rows = cache_result(
                    cache_key,
                    lambda: select(table_data, where_clause=where_clause),
                )
                _print_rows(schema, rows)
                continue

            print(f"Некорректное значение: {user_input}. Попробуйте снова.")
            continue

        # UPDATE
        if command == "update":
            if len(rest) < 6 or "set" not in rest or "where" not in rest:
                print(f"Некорректное значение: {user_input}. Попробуйте снова.")
                continue

            table_name = rest[0]
            schema = _get_schema(metadata, table_name)
            if schema is None:
                print(f'Ошибка: Таблица "{table_name}" не существует.')
                continue

            try:
                set_idx = rest.index("set")
                where_idx = rest.index("where")
            except ValueError:
                print(f"Некорректное значение: {user_input}. Попробуйте снова.")
                continue

            if set_idx != 1 or where_idx <= set_idx:
                print(f"Некорректное значение: {user_input}. Попробуйте снова.")
                continue

            set_part = rest[set_idx + 1 : where_idx]
            where_part = rest[where_idx + 1 :]

            set_assignment = _parse_assignment(set_part)
            where_assignment = _parse_assignment(where_part)
            if set_assignment is None or where_assignment is None:
                print(f"Некорректное значение: {user_input}. Попробуйте снова.")
                continue

            set_col, set_raw_val = set_assignment
            where_col, where_raw_val = where_assignment

            set_type = _get_col_type(schema, set_col)
            where_type = _get_col_type(schema, where_col)
            if set_type is None:
                print(f"Некорректное значение: {set_col}. Попробуйте снова.")
                continue
            if where_type is None:
                print(f"Некорректное значение: {where_col}. Попробуйте снова.")
                continue

            set_typed_val = _convert_value(set_raw_val, set_type)
            where_typed_val = _convert_value(where_raw_val, where_type)
            if set_typed_val is None:
                print(f"Некорректное значение: {set_raw_val}. Попробуйте снова.")
                continue
            if where_typed_val is None:
                print(f"Некорректное значение: {where_raw_val}. Попробуйте снова.")
                continue

            table_data = load_table_data(table_name)
            result = update(
                table_data,
                set_clause={set_col: set_typed_val},
                where_clause={where_col: where_typed_val},
            )
            if result is None:
                continue

            new_data, updated_ids = result
            if not updated_ids:
                print("Ошибка: Подходящие записи не найдены.")
                continue

            save_table_data(table_name, new_data)

            # Данные изменились — кэш select сбрасываем
            cache_result = create_cacher()

            if len(updated_ids) == 1:
                print(
                    f'Запись с ID={updated_ids[0]} в таблице "{table_name}" '
                    "успешно обновлена."
                )
            else:
                ids_str = ", ".join(map(str, updated_ids))
                print(
                    f'Записи ({ids_str}) в таблице "{table_name}" '
                    "успешно обновлены."
                )
            continue

        # DELETE
        if command == "delete":
            if len(rest) < 4 or rest[0] != "from" or rest[2] != "where":
                print(f"Некорректное значение: {user_input}. Попробуйте снова.")
                continue

            table_name = rest[1]
            schema = _get_schema(metadata, table_name)
            if schema is None:
                print(f'Ошибка: Таблица "{table_name}" не существует.')
                continue

            assignment = _parse_assignment(rest[3:])
            if assignment is None:
                print(f"Некорректное значение: {user_input}. Попробуйте снова.")
                continue

            col, raw_val = assignment
            col_type = _get_col_type(schema, col)
            if col_type is None:
                print(f"Некорректное значение: {col}. Попробуйте снова.")
                continue

            typed_val = _convert_value(raw_val, col_type)
            if typed_val is None:
                print(f"Некорректное значение: {raw_val}. Попробуйте снова.")
                continue

            table_data = load_table_data(table_name)
            result = delete(table_data, where_clause={col: typed_val})
            if result is None:
                continue

            new_data, deleted_ids = result
            if not deleted_ids:
                print("Ошибка: Подходящие записи не найдены.")
                continue

            save_table_data(table_name, new_data)

            # Данные изменились — кэш select сбрасываем
            cache_result = create_cacher()

            if len(deleted_ids) == 1:
                print(
                    f'Запись с ID={deleted_ids[0]} успешно удалена из таблицы '
                    f'"{table_name}".'
                )
            else:
                ids_str = ", ".join(map(str, deleted_ids))
                print(
                    f'Записи ({ids_str}) успешно удалены из таблицы "{table_name}".'
                )
            continue

        # Неизвестная команда
        print(f"Функции {command} нет. Попробуйте снова.")
