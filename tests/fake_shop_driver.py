"""Запуск приложения магазина отдельным процессом с подменённым драйвером.

    python fake_shop_driver.py <папка с shop.py>

Подключение всегда заканчивается ошибкой аутентификации: для проверок
запуска (потоки вывода, файл лога, путь к настройкам, ввод пароля без
терминала) база не нужна. Переменная FAKE_DRIVER_LOG задаёт файл, куда
дописываются аргументы connect и PG*-окружение в момент вызова.

Сам shop.py не меняется: он получает настоящий модуль psycopg, у которого
подменена только функция connect.
"""

import json
import os
import sys

import psycopg

LOG = os.environ.get("FAKE_DRIVER_LOG")
AUTH_FAILED = ('connection failed: FATAL:  password authentication failed for user "app_user"\n'
               'connection to server at "172.18.0.2", port 5432 failed')


def connect(*args, **kwargs):
    if LOG:
        record = {
            "args": [str(a) for a in args],
            "kwargs": {k: (v if isinstance(v, (int, str, type(None))) else str(v))
                       for k, v in kwargs.items()},
            "pg_env": {k: v for k, v in os.environ.items() if k.startswith("PG")},
        }
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    raise psycopg.OperationalError(AUTH_FAILED)


psycopg.connect = connect
sys.path.insert(0, sys.argv[1])
import shop  # noqa: E402

sys.exit(shop.main())
