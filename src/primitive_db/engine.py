from __future__ import annotations

import prompt


HELP_TEXT = """\
<command> exit - выйти из программы
<command> help - справочная информация
"""


def welcome() -> None:
    print("***")
    while True:
        command = prompt.string("Введите команду: ").strip()

        if command == "exit":
            break

        if command == "help":
            print(HELP_TEXT)
            continue

        print("Неизвестная команда. Введите help для справки.")
