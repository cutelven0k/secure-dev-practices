"""Практика 2: сервис-пингер PostgreSQL.

Раз в PING_INTERVAL_SECONDS секунд (по умолчанию 300) подключается к базе
и запрашивает версию. Успех пишется в stdout, ошибка в stderr.
Адрес базы берётся из config.json, логин и пароль из переменных окружения.
Каждая проверка идёт в дочернем процессе и ограничена по времени целиком.
"""

import json
import logging
import multiprocessing
import os
import re
import signal
import sys
import threading
import time
from pathlib import Path

import psycopg

CONFIG_PATH = Path(__file__).with_name("config.json")

ALLOWED_KEYS = {"host", "port", "dbname", "connect_timeout", "check_timeout"}
REQUIRED_KEYS = {"host", "port", "dbname"}
HOST_RE = re.compile(r"^[A-Za-z0-9.\-]{1,253}$")
DBNAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")

QUERY = "SELECT VERSION();"

# Шаблон типичного ответа на QUERY, например
# "PostgreSQL 18.6 (Debian 18.6-1.pgdg12+1) on x86_64-pc-linux-gnu, ..."
VERSION_RE = re.compile(r"^PostgreSQL 18\.\d+\b.* on \S+")

DEFAULT_INTERVAL = 300
DEFAULT_CHECK_TIMEOUT = 20

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
    check_timeout = data.get("check_timeout", DEFAULT_CHECK_TIMEOUT)
    if not isinstance(host, str) or not HOST_RE.match(host):
        raise ConfigError("неверное значение host")
    if type(port) is not int or not 1 <= port <= 65535:
        raise ConfigError("port должен быть числом от 1 до 65535")
    if not isinstance(dbname, str) or not DBNAME_RE.match(dbname):
        raise ConfigError("неверное значение dbname")
    if type(timeout) is not int or not 1 <= timeout <= 60:
        raise ConfigError("connect_timeout должен быть числом от 1 до 60")
    if type(check_timeout) is not int or not 1 <= check_timeout <= 120:
        raise ConfigError("check_timeout должен быть числом от 1 до 120")
    return {"host": host, "port": port, "dbname": dbname,
            "connect_timeout": timeout, "check_timeout": check_timeout}


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


def probe(cfg, user, password):
    """Одна проверка: подключение, запрос, сравнение с шаблоном.
    Возвращает (уровень, текст), сама в журнал не пишет."""
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
            row = conn.execute(QUERY).fetchone()
    except psycopg.Error as e:
        text = str(e).strip()
        return logging.ERROR, "БД недоступна: " + (text.splitlines()[0] if text else type(e).__name__)
    except OSError as e:
        return logging.ERROR, f"БД недоступна: ошибка сети: {e}"

    version = row[0] if row else None
    if isinstance(version, str) and VERSION_RE.match(version):
        return logging.INFO, f"БД доступна: {version}"
    return logging.WARNING, f"БД ответила, но ответ нетипичный: {version!r}"


def probe_in_child(conn, cfg, user, password):
    # сигналы остановки получает и обрабатывает родитель
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    try:
        result = probe(cfg, user, password)
    except Exception as e:
        result = logging.ERROR, f"непредвиденная ошибка при проверке: {e!r}"
    try:
        conn.send(result)
    finally:
        # без обработчиков atexit и сброса буферов, унаследованных от родителя
        os._exit(0)


def check_once(cfg, user, password, limit):
    """Проверка в дочернем процессе, не дольше limit секунд.

    connect_timeout не покрывает разрешение имени и ответ на сам запрос,
    а поток с зависшим вызовом libpq не прервать. Процесс можно убить."""
    ctx = multiprocessing.get_context("fork")
    parent_conn, child_conn = ctx.Pipe(duplex=False)
    proc = ctx.Process(target=probe_in_child, args=(child_conn, cfg, user, password))
    proc.start()
    child_conn.close()
    deadline = time.monotonic() + limit
    try:
        # ждём отрезками, чтобы между ними заметить SIGTERM
        while not stop_event.is_set():
            left = deadline - time.monotonic()
            if left <= 0:
                log.error("БД не ответила за %g с", limit)
                return
            if parent_conn.poll(min(0.5, left)):
                try:
                    level, text = parent_conn.recv()
                except EOFError:
                    log.error("проверка завершилась без результата")
                    return
                log.log(level, "%s", text)
                return
    finally:
        parent_conn.close()
        if proc.is_alive():
            proc.kill()
        # забираем потомка, иначе под PID 1 копились бы зомби
        proc.join()


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

    # проверка всегда короче интервала, чтобы не сдвигать следующую
    limit = min(cfg["check_timeout"], 0.8 * interval)
    log.info("пингер запущен: %s:%s/%s, интервал %s с, предел проверки %g с",
             cfg["host"], cfg["port"], cfg["dbname"], interval, limit)

    next_run = time.monotonic()
    while not stop_event.is_set():
        try:
            check_once(cfg, user, password, limit)
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
