from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any, TypeVar

import prompt

T = TypeVar("T")


def handle_db_errors(func: Callable[..., T]) -> Callable[..., T | None]:
    """
    Декоратор централизованной обработки ошибок БД.
    """

    def wrapper(*args: Any, **kwargs: Any) -> T | None:
        try:
            return func(*args, **kwargs)
        except FileNotFoundError:
            print(
                "Ошибка: Файл данных не найден." 
                "Возможно, база данных не инициализирована."
            )
        except KeyError as e:
            print(f"Ошибка: Таблица или столбец {e} не найден.")
        except ValueError as e:
            print(f"Ошибка валидации: {e}")
        except Exception as e:
            print(f"Произошла непредвиденная ошибка: {e}")
        return None

    return wrapper


def confirm_action(
        action_name: str,
        ) -> Callable[[Callable[..., T]], Callable[..., T | None]]:
    """
    Фабрика декораторов: спрашивает подтверждение у пользователя
    для опасных операций (удаление и т.п.).
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T | None]:
        def wrapper(*args: Any, **kwargs: Any) -> T | None:
            answer = prompt.string(
                f'Вы уверены, что хотите выполнить "{action_name}"? [y/n]: '
            ).strip().lower()

            if answer != "y":
                print("Операция отменена.")
                return None

            return func(*args, **kwargs)

        return wrapper

    return decorator


def log_time(func: Callable[..., T]) -> Callable[..., T]:
    """
    Декоратор для замера времени выполнения функции.
    """

    def wrapper(*args: Any, **kwargs: Any) -> T:
        start = time.monotonic()
        result = func(*args, **kwargs)
        elapsed = time.monotonic() - start
        print(f"Функция {func.__name__} выполнилась за {elapsed:.3f} секунд.")
        return result

    return wrapper


def create_cacher() -> Callable[[str, Callable[[], T]], T]:
    """
    Функция с замыканием для кэширования.
    """
    cache: dict[str, Any] = {}

    def cache_result(key: str, value_func: Callable[[], T]) -> T:
        if key in cache:
            return cache[key]
        value = value_func()
        cache[key] = value
        return value

    return cache_result
