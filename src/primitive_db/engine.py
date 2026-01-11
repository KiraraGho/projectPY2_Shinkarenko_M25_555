from __future__ import annotations

import shlex

import prompt

from src.primitive_db.core import create_table, drop_table, list_tables
from src.primitive_db.utils import load_metadata, save_metadata

# Файл для хранения метаданных базы данных
META_FILEPATH = "db_meta.json"


def print_help() -> None:
    """Выводит справочную информацию по командам."""
    print("\n***Процесс работы с таблицей***")
    print("Функции:")
    print("<command> create_table <имя_таблицы> <столбец1:тип> ... - создать таблицу")
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")
    print("\nОбщие команды:")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация\n")


def run() -> None:
    """
    Основной цикл программы.
    Принимает команды пользователя и вызывает соответствующую логику.
    """
    print("***Процесс работы с таблицей***")
    print_help()

    while True:
        user_input = prompt.string(">>>Введите команду: ").strip()

        # Разбор команды с учётом кавычек
        try:
            args = shlex.split(user_input)
        except ValueError:
            print(f"Некорректное значение: {user_input}. Попробуйте снова.")
            continue

        command = args[0]
        params = args[1:]

        # Выход из программы
        if command == "exit":
            break

        # Справка
        if command == "help":
            print_help()
            continue

        # Загружаем актуальные метаданные
        metadata = load_metadata(META_FILEPATH)

        # Создание таблицы
        if command == "create_table":
            if len(params) < 2:
                print("Некорректное значение: недостаточно аргументов." 
                      "Попробуйте снова.")
                continue

            table_name = params[0]
            columns = params[1:]

            metadata = create_table(metadata, table_name, columns)
            save_metadata(META_FILEPATH, metadata)
            continue

        # Удаление таблицы
        if command == "drop_table":
            if len(params) != 1:
                print("Некорректное значение: ожидается имя таблицы. Попробуйте снова.")
                continue

            metadata = drop_table(metadata, params[0])
            save_metadata(META_FILEPATH, metadata)
            continue

        # Список таблиц
        if command == "list_tables":
            list_tables(metadata)
            continue

        # Неизвестная команда
        print(f"Функции {command} нет. Попробуйте снова.")
