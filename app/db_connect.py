"""Практика 1: подключение к PostgreSQL и запрос SELECT VERSION().

Адрес БД берётся из файла config.json рядом с программой,
логин и пароль вводит пользователь.
"""

import getpass
import json
import os
import re
import sys
from pathlib import Path

import psycopg

CONFIG_PATH = Path(__file__).with_name("config.json")

# Из конфига принимаются только эти ключи. Всё остальное (options,
# sslmode, passfile, service и т.д.) считается ошибкой, иначе через файл
# можно было бы поменять параметры сессии, например work_mem или таймауты.
ALLOWED_KEYS = {"host", "port", "dbname", "connect_timeout"}
REQUIRED_KEYS = {"host", "port", "dbname"}

HOST_RE = re.compile(r"^[A-Za-z0-9.\-]{1,253}$")
DBNAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


class ConfigError(Exception):
    """Ошибка в файле настроек, текст показывается пользователю."""


def load_config(path=CONFIG_PATH):
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        raise ConfigError(f"не найден файл настроек {path}") from None
    except json.JSONDecodeError as e:
        raise ConfigError(
            f"файл настроек повреждён: строка {e.lineno}, столбец {e.colno}"
        ) from None
    except (OSError, UnicodeDecodeError):
        raise ConfigError(f"не удалось прочитать файл настроек {path}") from None

    if not isinstance(data, dict):
        raise ConfigError("в файле настроек ожидается объект JSON")

    extra = set(data) - ALLOWED_KEYS
    if extra:
        raise ConfigError("недопустимые параметры в настройках: " + ", ".join(sorted(extra)))
    missing = REQUIRED_KEYS - set(data)
    if missing:
        raise ConfigError("в настройках не хватает параметров: " + ", ".join(sorted(missing)))

    host, port, dbname = data["host"], data["port"], data["dbname"]
    timeout = data.get("connect_timeout", 5)

    if not isinstance(host, str) or not HOST_RE.match(host):
        raise ConfigError("неверное значение host")
    # bool в Python наследуется от int, его отсекаем отдельно
    if type(port) is not int or not 1 <= port <= 65535:
        raise ConfigError("port должен быть числом от 1 до 65535")
    if not isinstance(dbname, str) or not DBNAME_RE.match(dbname):
        raise ConfigError("неверное значение dbname")
    if type(timeout) is not int or not 1 <= timeout <= 60:
        raise ConfigError("connect_timeout должен быть числом от 1 до 60")

    return {"host": host, "port": port, "dbname": dbname, "connect_timeout": timeout}


def check_credential(value, name):
    if not value:
        raise ValueError(f"{name} не может быть пустым")
    if "\x00" in value:
        raise ValueError(f"{name} содержит недопустимый символ")
    return value


def ask_credentials():
    user = check_credential(input("Логин: ").strip(), "логин")
    # getpass не показывает пароль на экране и не сохраняет его в истории
    password = check_credential(getpass.getpass("Пароль: "), "пароль")
    return user, password


def clear_pg_env():
    # libpq сам читает переменные PGOPTIONS, PGSERVICE, PGPASSFILE и т.д.
    # Чтобы через окружение нельзя было подмешать свои параметры, убираем их.
    for key in list(os.environ):
        if key.startswith("PG"):
            del os.environ[key]


def connect(cfg, user, password):
    clear_pg_env()
    # Защита от инъекции в параметры подключения (connection-string injection):
    # каждое значение идёт отдельным именованным параметром, conninfo psycopg
    # собирает и экранирует сам, поэтому пароль  x' host=evil options='-c work_mem=4GB
    # останется просто паролем и не добавит новых параметров.
    return psycopg.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname=cfg["dbname"],
        connect_timeout=cfg["connect_timeout"],
        user=user,
        password=password,
        passfile=os.devnull,
        application_name="practice1",
    )


def main():
    try:
        cfg = load_config()
    except ConfigError as e:
        print(f"Ошибка настроек: {e}", file=sys.stderr)
        return 1

    try:
        user, password = ask_credentials()
    except ValueError as e:
        print(f"Ошибка ввода: {e}", file=sys.stderr)
        return 1
    except (EOFError, KeyboardInterrupt):
        print("\nВвод отменён", file=sys.stderr)
        return 1

    try:
        with connect(cfg, user, password) as conn:
            version = conn.execute("SELECT VERSION();").fetchone()[0]
    except psycopg.OperationalError:
        print(
            "Не удалось подключиться к базе данных: проверьте логин, пароль "
            "и доступность сервера",
            file=sys.stderr,
        )
        return 2
    except psycopg.Error:
        print("Ошибка при выполнении запроса к базе данных", file=sys.stderr)
        return 3

    print(f"Подключение успешно. Версия сервера: {version}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
