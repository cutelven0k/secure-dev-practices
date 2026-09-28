"""Практика 2: сервис-пингер PostgreSQL.

Раз в PING_INTERVAL_SECONDS секунд (по умолчанию 300) подключается к базе
и запрашивает версию. Успех пишется в stdout, ошибка в stderr.
Адрес базы берётся из config.json, логин и пароль из переменных окружения.
"""

import json
import logging
import os
import re
import signal
import sys
import threading
import time
from pathlib import Path

import psycopg

CONFIG_PATH = Path(os.environ.get("PINGER_CONFIG", Path(__file__).with_name("config.json")))

ALLOWED_KEYS = {"host", "port", "dbname", "connect_timeout"}
REQUIRED_KEYS = {"host", "port", "dbname"}
HOST_RE = re.compile(r"^[A-Za-z0-9.\-]{1,253}$")
DBNAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")

# Так начинается нормальный ответ SELECT version(), например
# "PostgreSQL 18.6 (Debian 18.6-1.pgdg12+1) on x86_64-pc-linux-gnu, ..."
VERSION_RE = re.compile(r"^PostgreSQL \d+(\.\d+)?\b.* on \S+")

DEFAULT_INTERVAL = 300

log = logging.getLogger("pinger")
stop_event = threading.Event()


class ConfigError(Exception):
    pass


def load_config(path=CONFIG_PATH):
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        raise ConfigError(f"не найден файл настроек {path}") from None
    except json.JSONDecodeError as e:
        raise ConfigError(f"файл настроек повреждён: строка {e.lineno}, столбец {e.colno}") from None
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
    if type(port) is not int or not 1 <= port <= 65535:
        raise ConfigError("port должен быть числом от 1 до 65535")
    if not isinstance(dbname, str) or not DBNAME_RE.match(dbname):
        raise ConfigError("неверное значение dbname")
    if type(timeout) is not int or not 1 <= timeout <= 60:
        raise ConfigError("connect_timeout должен быть числом от 1 до 60")
    return {"host": host, "port": port, "dbname": dbname, "connect_timeout": timeout}


def load_env():
    raw = os.environ.get("PING_INTERVAL_SECONDS", str(DEFAULT_INTERVAL)).strip()
    if not raw.isdigit() or not 1 <= int(raw) <= 86400:
        raise ConfigError("PING_INTERVAL_SECONDS должен быть целым числом от 1 до 86400")

    user = os.environ.get("DB_USER", "")
    password = os.environ.get("DB_PASSWORD", "")
    if not user or not password:
        raise ConfigError("не заданы DB_USER и DB_PASSWORD")
    if "\x00" in user or "\x00" in password:
        raise ConfigError("DB_USER или DB_PASSWORD содержат недопустимый символ")

    return int(raw), user, password, os.environ.get("LOG_FILE", "").strip()


class MaxLevelFilter(logging.Filter):
    def __init__(self, max_level):
        super().__init__()
        self.max_level = max_level

    def filter(self, record):
        return record.levelno <= self.max_level


def setup_logging(log_file):
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    # INFO и WARNING (успех и нетипичный ответ) идут в stdout
    out = logging.StreamHandler(sys.stdout)
    out.setLevel(logging.INFO)
    out.addFilter(MaxLevelFilter(logging.WARNING))
    out.setFormatter(fmt)

    # ERROR (не удалось подключиться) идёт в stderr
    err = logging.StreamHandler(sys.stderr)
    err.setLevel(logging.ERROR)
    err.setFormatter(fmt)

    log.setLevel(logging.INFO)
    log.addHandler(out)
    log.addHandler(err)

    if log_file:
        # в файл дублируется всё подряд
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        log.addHandler(fh)


def check_once(cfg, user, password):
    try:
        with psycopg.connect(
            host=cfg["host"],
            port=cfg["port"],
            dbname=cfg["dbname"],
            user=user,
            password=password,
            passfile=os.devnull,
            application_name="pinger",
            # сколько ждать установки соединения; без этого при потере
            # пакетов в сети подключение может висеть очень долго
            connect_timeout=cfg["connect_timeout"],
            # если соединение уже есть, но ответы перестали приходить:
            # keepalive-пакеты и tcp_user_timeout (мс) обрывают его,
            # а не ждут стандартные для TCP два часа
            keepalives=1,
            keepalives_idle=5,
            keepalives_interval=2,
            keepalives_count=3,
            tcp_user_timeout=cfg["connect_timeout"] * 1000,
        ) as conn:
            row = conn.execute("SELECT version()").fetchone()
    except psycopg.Error as e:
        log.error("БД недоступна: %s", str(e).strip().splitlines()[0] if str(e).strip() else type(e).__name__)
        return
    except OSError as e:
        log.error("БД недоступна: ошибка сети: %s", e)
        return

    version = row[0] if row else None
    if isinstance(version, str) and VERSION_RE.match(version):
        log.info("БД доступна: %s", version)
    else:
        log.warning("БД ответила, но ответ нетипичный: %r", version)


received_signal = None


def handle_signal(signum, _frame):
    # в обработчике сигнала только ставим флаг, логировать отсюда небезопасно
    global received_signal
    received_signal = signum
    stop_event.set()


def clear_pg_env():
    for key in list(os.environ):
        if key.startswith("PG"):
            del os.environ[key]


def main():
    try:
        interval, user, password, log_file = load_env()
        cfg = load_config()
        setup_logging(log_file)
    except ConfigError as e:
        print(f"Ошибка настроек: {e}", file=sys.stderr)
        return 1
    except OSError as e:
        print(f"Не удалось открыть файл лога: {e}", file=sys.stderr)
        return 1

    clear_pg_env()
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    log.info("пингер запущен: %s:%s/%s, интервал %s с",
             cfg["host"], cfg["port"], cfg["dbname"], interval)

    next_run = time.monotonic()
    while not stop_event.is_set():
        try:
            check_once(cfg, user, password)
        except Exception:
            # что бы ни случилось в проверке, следующая всё равно будет
            log.exception("непредвиденная ошибка при проверке")
        # считаем от начала проверки, чтобы интервал не "уплывал"
        next_run += interval
        now = time.monotonic()
        if next_run < now:
            # проверка заняла дольше интервала, пропущенные запуски не догоняем
            next_run = now
        stop_event.wait(next_run - now)

    log.info("получен сигнал %s, завершаю работу", signal.Signals(received_signal).name)
    return 0


if __name__ == "__main__":
    sys.exit(main())
