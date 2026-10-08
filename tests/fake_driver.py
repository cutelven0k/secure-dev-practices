"""Запуск пингера отдельным процессом с подменённым драйвером.

    python fake_driver.py <папка с pinger.py>

Поведение драйвера задаётся переменными окружения:
  FAKE_DRIVER       ok (ответ PostgreSQL 18), fail (ошибка подключения),
                    first_fail (первая проверка с ошибкой, дальше ok),
                    atypical (ответ PostgreSQL 17), hang (подключение молчит),
                    hang_query (подключение есть, ответа на запрос нет)
  FAKE_DRIVER_LOG   файл, куда дописываются аргументы connect и PG*-окружение
  FAKE_DRIVER_STATE файл-отметка для first_fail (общий для всех процессов)

Сам пингер не меняется: он получает настоящий модуль psycopg, у которого
подменена только функция connect.
"""

import json
import os
import sys
import time

import psycopg

MODE = os.environ.get("FAKE_DRIVER", "ok")
LOG = os.environ.get("FAKE_DRIVER_LOG")
STATE = os.environ.get("FAKE_DRIVER_STATE")

VERSION_18 = ("PostgreSQL 18.6 (Debian 18.6-1.pgdg12+1) on x86_64-pc-linux-gnu, "
              "compiled by gcc (Debian 12.2.0-14+deb12u1) 12.2.0, 64-bit")
VERSION_17 = ("PostgreSQL 17.5 (Debian 17.5-1.pgdg120+1) on x86_64-pc-linux-gnu, "
              "compiled by gcc (Debian 12.2.0-14) 12.2.0, 64-bit")
REFUSED = ('connection failed: connection to server at "172.18.0.2", port 5432 failed: '
           'Connection refused\n\tIs the server running on that host and accepting TCP/IP connections?')


class Connection:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, query, *args, **kwargs):
        if MODE == "hang_query":
            time.sleep(3600)
        return self

    def fetchone(self):
        return (VERSION_17 if MODE == "atypical" else VERSION_18,)

    def close(self):
        pass


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
    if MODE == "hang":
        time.sleep(3600)
    if MODE == "fail":
        raise psycopg.OperationalError(REFUSED)
    if MODE == "first_fail":
        if not os.path.exists(STATE):
            with open(STATE, "w"):
                pass
            raise psycopg.OperationalError(REFUSED)
    return Connection()


psycopg.connect = connect
sys.path.insert(0, sys.argv[1])
import pinger  # noqa: E402

sys.exit(pinger.main())
