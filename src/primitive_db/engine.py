from __future__ import annotations

import shlex

import prompt
from prettytable import PrettyTable

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

# Файл, где лежат метаданные (список таблиц и их схема)
META_FILEPATH = "db_meta.json"


def print_help() -> None:
    """Печатает справку по командам """
    print("\n***Операции с данными***\n")
    print("Функции:")
    print(
        '<command> insert into <имя_таблицы> values (<значение1>, <значение2>, ...)" ' \
        '"- создать запись.'
    )
    print(
        "<command> select from <имя_таблицы> where <столбец> = <значение>" 
        "- прочитать записи по условию."
    )
    print("<command> select from <имя_таблицы> - прочитать все записи.")
    print(
        "<command> update <имя_таблицы> set <столбец> = <новое_значение> "
        "where <столбец> = <значение> - обновить записи."
    )
    print(
        "<command> delete from <имя_таблицы> where <столбец> = <значение>" 
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


def _get_schema(metadata: dict, table_name: str) -> list[dict[str, str]] | None:
    """Достаёт схему таблицы из метаданных."""
    tables = metadata.get("tables", {})
    table = tables.get(table_name)
    if not table:
        return None
    return table.get("columns", [])


def _get_col_type(schema: list[dict[str, str]], col_name: str) -> str | None:
    """Возвращает тип столбца по имени (int/str/bool) или None."""
    for col in schema:
        if col.get("name") == col_name:
            return col.get("type")
    return None


def _convert_value(raw: str, expected_type: str) -> object | None:
    """
    Приводит строковое значение к типу.
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
    """
    Парсит часть values(...).
    """
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

        # Разделитель запятыми учитываем только если мы НЕ внутри кавычек
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


def _parse_assignment(expr_tokens: list[str]) -> tuple[str, str] | None:
    """
    Парсит присваивание вида: <col> = <value>
    На вход получает токены (после shlex.split).
    """
    # Поддерживаем варианты:
    #   age = 28        -> ["age","=","28"]
    #   age=28          -> ["age=28"]
    #   name="Sergei"   -> ['name="Sergei"']
    if not expr_tokens:
        return None

    if len(expr_tokens) == 1 and "=" in expr_tokens[0]:
        left, right = expr_tokens[0].split("=", 1)
        left = left.strip()
        right = right.strip()
        if not left or not right:
            return None
        return left, right

    if len(expr_tokens) >= 3 and expr_tokens[1] == "=":
        left = expr_tokens[0].strip()
        right = expr_tokens[2].strip()
        if not left or not right:
            return None
        return left, right

    return None


def _print_rows(schema: list[dict[str, str]], rows: list[dict]) -> None:
    """Печатает результат SELECT в виде таблицы PrettyTable."""
    if not rows:
        print("(записей нет)")
        return

    # Порядок колонок берём из схемы
    headers = [c["name"] for c in schema]
    table = PrettyTable()
    table.field_names = headers

    for row in rows:
        table.add_row([row.get(h) for h in headers])

    print(table)


def run() -> None:
    """
    Главная функция запуска базы данных.
    Содержит цикл чтения команд и вызов соответствующей логики.
    """
    print("***Операции с данными***")
    print_help()

    while True:
        user_input = prompt.string(">>> Введите команду: ").strip()

        # На всякий случай: если вдруг пустая строка (prompt обычно не пропускает)
        if not user_input:
            continue

        # Разбираем команду безопасно (учитывает кавычки)
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

        # Всегда берём актуальные метаданные
        metadata = load_metadata(META_FILEPATH)

        # Управление таблицами
        if command == "create_table":
            if len(rest) < 2:
                print("Некорректное значение: недостаточно аргументов." 
                      "Попробуйте снова.")
                continue

            table_name = rest[0]
            columns = rest[1:]
            metadata = create_table(metadata, table_name, columns)
            save_metadata(META_FILEPATH, metadata)
            continue

        if command == "drop_table":
            if len(rest) != 1:
                print("Некорректное значение: ожидается имя таблицы." 
                      "Попробуйте снова.")
                continue

            table_name = rest[0]
            metadata = drop_table(metadata, table_name)
            save_metadata(META_FILEPATH, metadata)
            continue

        if command == "list_tables":
            if rest:
                print("Некорректное значение: лишние аргументы. Попробуйте снова.")
                continue

            list_tables(metadata)
            continue

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


        if command == "insert":
            if len(rest) < 4 or rest[0] != "into" or rest[2] != "values":
                print(f"Некорректное значение: {user_input}. Попробуйте снова.")
                continue

            table_name = rest[1]
            schema = _get_schema(metadata, table_name)
            if schema is None:
                print(f'Ошибка: Таблица "{table_name}" не существует.')
                continue

            # values часть может быть разбита на несколько токенов, склеим обратно
            values_part = " ".join(rest[3:])
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
            print(f'Запись с ID={new_id} успешно добавлена в таблицу "{table_name}".')
            continue


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

            # Без where
            if len(rest) == 2:
                rows = select(table_data, where_clause=None)
                _print_rows(schema, rows)
                continue

            # С where
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

                rows = select(table_data, where_clause={col: typed_val})
                _print_rows(schema, rows)
                continue

            print(f"Некорректное значение: {user_input}. Попробуйте снова.")
            continue


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

            if not (set_idx == 1 and where_idx > set_idx):
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
            new_data, updated_ids = update(
                table_data,
                set_clause={set_col: set_typed_val},
                where_clause={where_col: where_typed_val},
            )

            if not updated_ids:
                print("Ошибка: Подходящие записи не найдены.")
                continue

            save_table_data(table_name, new_data)

            if len(updated_ids) == 1:
                print(f'Запись с ID={updated_ids[0]} в таблице "{table_name}"' 
                      "успешно обновлена.")
            else:
                ids_str = ", ".join(map(str, updated_ids))
                print(
                    f'Записи ({ids_str}) в таблице "{table_name}" '
                    "успешно обновлены."
                )
            continue


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
            new_data, deleted_ids = delete(table_data, where_clause={col: typed_val})

            if not deleted_ids:
                print("Ошибка: Подходящие записи не найдены.")
                continue

            save_table_data(table_name, new_data)

            if len(deleted_ids) == 1:
                print(f'Запись с ID={deleted_ids[0]} успешно удалена из таблицы' 
                      '"{table_name}".')
            else:
                ids_str = ", ".join(map(str, deleted_ids))
                print(
                    f'Записи ({ids_str}) успешно удалены из таблицы "{table_name}".'
                )
            continue

        print(f"Функции {command} нет. Попробуйте снова.")
