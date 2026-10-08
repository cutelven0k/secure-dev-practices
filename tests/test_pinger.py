"""Проверки пингера (практика 2).

Две группы:
- в своём процессе: probe() (запрос, ответ драйвера, шаблон версии, инъекции
  через логин и пароль), check_once() (предел времени проверки), main()
  с подменёнными часами (расписание) и с подменённой проверкой (живучесть цикла),
  load_env()/load_config() (значения переменных и файла);
- отдельным процессом через tests/fake_driver.py: потоки stdout/stderr, файл
  лога, интервал из окружения, предел при молчащей сети, SIGTERM, коды выхода
  при неверных настройках. Так видно ровно то, что увидит преподаватель
  в `docker logs`.
"""

import json
import logging
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo

REPO = Path(__file__).resolve().parents[1]
PINGER_DIR = REPO / "pinger"
RUNNER = Path(__file__).with_name("fake_driver.py")
sys.path.insert(0, str(PINGER_DIR))
import pinger  # noqa: E402

CFG = {"host": "db", "port": 5432, "dbname": "practice_db", "connect_timeout": 5, "check_timeout": 20}
FILE_CFG = {"host": "db", "port": 5432, "dbname": "practice_db", "connect_timeout": 5}
QUERY = "SELECT VERSION();"
VERSION_18 = ("PostgreSQL 18.6 (Debian 18.6-1.pgdg12+1) on x86_64-pc-linux-gnu, "
              "compiled by gcc (Debian 12.2.0-14+deb12u1) 12.2.0, 64-bit")
ATYPICAL = [
    "PostgreSQL 17.5 (Debian 17.5-1.pgdg120+1) on x86_64-pc-linux-gnu, compiled by gcc, 64-bit",
    "PostgreSQL 180.1 on x86_64-pc-linux-gnu",
    "MySQL 8.0.36",
    "",
    None,
    42,
]
ATTACKS = [
    "x host=evil",
    "x' host=evil options='-c work_mem=4GB",
    "x' passfile='/etc/passwd",
    "options=-c work_mem=4GB",
    "host=evil host=other",
    "a'\"\\b",
    "two words",
    "  password with edge spaces  ",
    "postgresql://evil/other?host=evil&options=-c%20work_mem=4GB",
]
# Ключи, которые пингер вправе передать драйверу. Ничего сверх: ни options,
# ни service, ни statement_timeout, ни check_timeout.
DRIVER_KEYS = {
    "host", "port", "dbname", "user", "password", "passfile", "application_name",
    "connect_timeout", "keepalives", "keepalives_idle", "keepalives_interval",
    "keepalives_count", "tcp_user_timeout",
}
EXTRA_KEYS = ["user", "password", "options", "statement_timeout", "passfile", "search_path"]
BROKEN_JSON = [json.dumps(FILE_CFG)[:-1], json.dumps(FILE_CFG).replace(",", "", 1)]


# ---------- общие помощники ----------

class FakeConn:
    def __init__(self, answer, rows=True):
        self.answer = answer
        self.rows = rows
        self.queries = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, query, *args, **kwargs):
        self.queries.append(query)
        return self

    def fetchone(self):
        return (self.answer,) if self.rows else None


@pytest.fixture(autouse=True)
def isolated_pinger(monkeypatch):
    # Окружение, обработчики логгера, флаг остановки и обработчики сигналов
    # возвращаются после каждого теста.
    monkeypatch.setattr(os, "environ", os.environ.copy())
    for key in list(os.environ):
        if key.startswith("PG"):
            del os.environ[key]
    monkeypatch.setenv("DB_USER", "app_user")
    monkeypatch.setenv("DB_PASSWORD", "ordinary password")
    monkeypatch.delenv("PING_INTERVAL_SECONDS", raising=False)
    monkeypatch.delenv("LOG_FILE", raising=False)
    handlers = list(pinger.log.handlers)
    old_term, old_int = signal.getsignal(signal.SIGTERM), signal.getsignal(signal.SIGINT)
    yield
    pinger.log.handlers[:] = handlers
    pinger.stop_event.clear()
    pinger.received_signal = None
    for signum, old in ((signal.SIGTERM, old_term), (signal.SIGINT, old_int)):
        if old is not None:
            signal.signal(signum, old)


@pytest.fixture
def records():
    got = []

    class Collect(logging.Handler):
        def emit(self, record):
            got.append(record)

    handler = Collect()
    pinger.log.addHandler(handler)
    pinger.log.setLevel(logging.INFO)
    yield got
    pinger.log.removeHandler(handler)


def capture_connect(monkeypatch, conn):
    calls = []

    def fake_connect(*args, **kwargs):
        calls.append((args, kwargs))
        return conn

    monkeypatch.setattr(pinger.psycopg, "connect", fake_connect)
    return calls


def write_config(directory, data):
    path = Path(directory) / "config.json"
    path.write_text(data if isinstance(data, str) else json.dumps(data), encoding="utf-8")
    return path


def pinger_copy(tmp_path, config):
    """Копия pinger.py рядом со своим config.json: путь к файлу не настраивается."""
    directory = tmp_path / "pinger"
    directory.mkdir()
    shutil.copy(PINGER_DIR / "pinger.py", directory / "pinger.py")
    write_config(directory, config)
    return directory


def no_children_left():
    """После проверки у процесса нет ни живых, ни незабранных дочерних процессов."""
    try:
        os.waitpid(-1, os.WNOHANG)
    except ChildProcessError:
        return True
    return False


def processes_mentioning(text):
    found = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            if text in (entry / "cmdline").read_text(errors="ignore"):
                found.append(entry.name)
        except OSError:
            continue
    return found


# ---------- probe(): одна проверка без журнала ----------

def test_probe_sends_exact_query_and_separate_credentials(monkeypatch):
    conn = FakeConn(VERSION_18)
    calls = capture_connect(monkeypatch, conn)
    level, text = pinger.probe(CFG, "app_user", "ordinary password")
    assert conn.queries == [QUERY]
    assert level == logging.INFO
    assert VERSION_18 in text
    (args, kwargs), = calls
    assert args == ()
    assert kwargs["user"] == "app_user"
    assert kwargs["password"] == "ordinary password"
    assert {k: kwargs[k] for k in FILE_CFG} == FILE_CFG
    assert kwargs["passfile"] == os.devnull
    assert set(kwargs) <= DRIVER_KEYS


@pytest.mark.parametrize("answer", ATYPICAL, ids=["pg17", "pg180", "mysql", "empty", "none", "int"])
def test_probe_atypical_answer_is_warning(monkeypatch, answer):
    capture_connect(monkeypatch, FakeConn(answer))
    level, text = pinger.probe(CFG, "app_user", "ordinary password")
    assert level == logging.WARNING
    assert repr(answer) in text or str(answer) in text


def test_probe_no_rows_is_warning(monkeypatch):
    capture_connect(monkeypatch, FakeConn(None, rows=False))
    level, _ = pinger.probe(CFG, "app_user", "ordinary password")
    assert level == logging.WARNING


@pytest.mark.parametrize("error", [
    psycopg.OperationalError('connection failed: connection to server at "172.18.0.2", port 5432 '
                             'failed: Connection refused'),
    psycopg.OperationalError("connection timeout expired"),
    OSError("[Errno 101] Network is unreachable"),
], ids=["refused", "timeout", "oserror"])
def test_probe_connection_failure_is_error(monkeypatch, error):
    def fail_connect(*args, **kwargs):
        raise error

    monkeypatch.setattr(pinger.psycopg, "connect", fail_connect)
    level, text = pinger.probe(CFG, "app_user", "ordinary password")
    assert level == logging.ERROR
    assert str(error).splitlines()[0] in text


@pytest.mark.parametrize("field", ["user", "password"])
@pytest.mark.parametrize("attack", ATTACKS)
def test_probe_keeps_credential_in_its_field(monkeypatch, field, attack):
    credentials = {"user": "app_user", "password": "ordinary password", field: attack}
    calls = capture_connect(monkeypatch, FakeConn(VERSION_18))
    cfg = dict(CFG)
    pinger.probe(cfg, credentials["user"], credentials["password"])
    (args, kwargs), = calls
    assert args == ()
    assert kwargs["user"] == credentials["user"]
    assert kwargs["password"] == credentials["password"]
    assert {k: kwargs[k] for k in FILE_CFG} == FILE_CFG
    assert set(kwargs) <= DRIVER_KEYS
    assert cfg == CFG
    # Настоящий драйвер собирает строку подключения и разбирает её обратно:
    # ввод остаётся в своём поле, новых опций не появляется.
    parsed = conninfo_to_dict(make_conninfo(**kwargs))
    assert parsed == {k: str(v) for k, v in kwargs.items()}


def test_probe_does_not_pass_check_timeout_to_driver(monkeypatch):
    calls = capture_connect(monkeypatch, FakeConn(VERSION_18))
    pinger.probe({**CFG, "check_timeout": 7}, "app_user", "ordinary password")
    (_, kwargs), = calls
    assert "check_timeout" not in kwargs
    assert kwargs["connect_timeout"] == 5


# ---------- check_once(): предел времени ----------

def test_check_once_logs_result_in_parent(monkeypatch, records):
    capture_connect(monkeypatch, FakeConn(VERSION_18))
    pinger.check_once(CFG, "app_user", "ordinary password", 5)
    assert [r.levelno for r in records] == [logging.INFO]
    assert VERSION_18 in records[0].getMessage()
    assert no_children_left()


@pytest.mark.parametrize("where", ["connect", "execute"])
def test_check_once_kills_hung_check_within_limit(monkeypatch, records, where):
    class HangConn(FakeConn):
        def execute(self, query, *args, **kwargs):
            time.sleep(3600)
            return self

    def hang_connect(*args, **kwargs):
        if where == "connect":
            time.sleep(3600)
        return HangConn(VERSION_18)

    monkeypatch.setattr(pinger.psycopg, "connect", hang_connect)
    started = time.monotonic()
    pinger.check_once(CFG, "app_user", "ordinary password", 2)
    elapsed = time.monotonic() - started
    assert elapsed <= 4, elapsed
    assert [r.levelno for r in records] == [logging.ERROR]
    assert re.search(r"не ответила за 2(\.0)? с", records[0].getMessage())
    assert no_children_left()


def test_check_once_reports_fast_failure_without_waiting(monkeypatch, records):
    def fail_connect(*args, **kwargs):
        raise psycopg.OperationalError("connection timeout expired")

    monkeypatch.setattr(pinger.psycopg, "connect", fail_connect)
    started = time.monotonic()
    pinger.check_once(CFG, "app_user", "ordinary password", 20)
    assert time.monotonic() - started < 3
    assert [r.levelno for r in records] == [logging.ERROR]
    assert no_children_left()


# ---------- main() в своём процессе: расписание и живучесть цикла ----------

class FakeClock:
    """Часы и событие остановки под управлением теста: время идёт только
    когда пингер ждёт или когда «проверка» его тратит."""

    def __init__(self, stop_after):
        self.now = 1000.0
        self.flag = False
        self.checks = 0
        self.stop_after = stop_after

    def is_set(self):
        return self.flag

    def set(self):
        self.flag = True

    def clear(self):
        self.flag = False

    def wait(self, timeout=None):
        if timeout:
            self.now += max(timeout, 0)
        if self.checks >= self.stop_after:
            pinger.handle_signal(signal.SIGTERM, None)
        return self.flag


def run_main_with_fake_checks(monkeypatch, interval, durations=(), outcomes=()):
    clock = FakeClock(stop_after=max(len(durations), len(outcomes)))
    starts = []
    pending_durations = list(durations)
    pending_outcomes = list(outcomes)

    def fake_check(cfg, user, password, *args, **kwargs):
        starts.append(clock.now - 1000.0)
        clock.checks += 1
        if pending_durations:
            clock.now += pending_durations.pop(0)
        if pending_outcomes:
            outcome = pending_outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome

    monkeypatch.setattr(pinger.time, "monotonic", lambda: clock.now)
    monkeypatch.setattr(pinger, "stop_event", clock)
    monkeypatch.setattr(pinger, "check_once", fake_check)
    monkeypatch.setenv("PING_INTERVAL_SECONDS", str(interval))
    rc = pinger.main()
    return rc, starts


def test_schedule_counts_from_start_of_check(monkeypatch, capsys):
    # 3 с, 8 с (предел при интервале 10), мгновенно, 7,9 с — начала всё равно
    # идут с шагом ровно в интервал.
    rc, starts = run_main_with_fake_checks(monkeypatch, 10, durations=[3, 8, 0, 7.9])
    assert rc == 0
    assert starts == [0, 10, 20, 30]
    out = capsys.readouterr()
    assert "получен сигнал SIGTERM" in out.out
    assert "Traceback" not in out.err


def test_loop_survives_failed_checks(monkeypatch, capsys):
    outcomes = [RuntimeError("boom"), psycopg.OperationalError("connection refused"), None, None]
    rc, starts = run_main_with_fake_checks(monkeypatch, 300, outcomes=outcomes)
    assert rc == 0
    assert starts == [0, 300, 600, 900]
    assert "Traceback" not in capsys.readouterr().out


def test_loop_default_interval_is_300(monkeypatch, capsys):
    monkeypatch.delenv("PING_INTERVAL_SECONDS", raising=False)
    clock = FakeClock(stop_after=2)
    starts = []

    def fake_check(*args, **kwargs):
        starts.append(clock.now - 1000.0)
        clock.checks += 1

    monkeypatch.setattr(pinger.time, "monotonic", lambda: clock.now)
    monkeypatch.setattr(pinger, "stop_event", clock)
    monkeypatch.setattr(pinger, "check_once", fake_check)
    assert pinger.main() == 0
    assert starts == [0, 300]


# ---------- load_env(), load_config() ----------

@pytest.mark.parametrize("value,expected", [("300", 300), ("10", 10), ("1", 1), (None, 300)])
def test_interval_env_accepted(monkeypatch, value, expected):
    if value is not None:
        monkeypatch.setenv("PING_INTERVAL_SECONDS", value)
    assert pinger.load_env()[0] == expected


@pytest.mark.parametrize("value", ["abc", "0", "-5", "5.5", "", "5m", " "])
def test_interval_env_rejected(monkeypatch, value):
    monkeypatch.setenv("PING_INTERVAL_SECONDS", value)
    with pytest.raises(pinger.ConfigError, match="PING_INTERVAL_SECONDS"):
        pinger.load_env()


@pytest.mark.parametrize("missing", ["DB_USER", "DB_PASSWORD"])
def test_credentials_required(monkeypatch, missing):
    monkeypatch.delenv(missing)
    with pytest.raises(pinger.ConfigError, match=missing):
        pinger.load_env()


def test_env_credentials_are_taken_verbatim(monkeypatch):
    password = "  x' passfile='/etc/passwd host=evil  "
    monkeypatch.setenv("DB_USER", "app_user")
    monkeypatch.setenv("DB_PASSWORD", password)
    _, user, got, _ = pinger.load_env()
    assert (user, got) == ("app_user", password)


def test_log_file_env_is_optional(monkeypatch):
    assert pinger.load_env()[3] == ""
    monkeypatch.setenv("LOG_FILE", "/logs/pinger.log")
    assert pinger.load_env()[3] == "/logs/pinger.log"


def test_config_defaults(tmp_path):
    cfg = pinger.load_config(write_config(tmp_path, {"host": "db", "port": 5432, "dbname": "practice_db"}))
    assert cfg["connect_timeout"] == 5
    assert cfg["check_timeout"] == 20


@pytest.mark.parametrize("value", [1, 20, 120])
def test_config_check_timeout_accepted(tmp_path, value):
    cfg = pinger.load_config(write_config(tmp_path, {**FILE_CFG, "check_timeout": value}))
    assert cfg["check_timeout"] == value


@pytest.mark.parametrize("value", [0, 121, "20", True])
def test_config_check_timeout_rejected(tmp_path, value):
    with pytest.raises(pinger.ConfigError, match="check_timeout"):
        pinger.load_config(write_config(tmp_path, {**FILE_CFG, "check_timeout": value}))


@pytest.mark.parametrize("extra", EXTRA_KEYS)
def test_config_rejects_credentials_and_extra_options(tmp_path, extra):
    with pytest.raises(pinger.ConfigError, match=f"недопустимые параметры.*{extra}"):
        pinger.load_config(write_config(tmp_path, {**FILE_CFG, extra: "injected"}))


@pytest.mark.parametrize("content", BROKEN_JSON, ids=["missing-brace", "missing-comma"])
def test_config_reports_broken_json(tmp_path, content):
    with pytest.raises(pinger.ConfigError, match="файл настроек повреждён"):
        pinger.load_config(write_config(tmp_path, content))


def test_repo_config_has_no_credentials():
    data = json.loads((PINGER_DIR / "config.json").read_text(encoding="utf-8"))
    assert "user" not in data and "password" not in data
    assert set(data) <= {"host", "port", "dbname", "connect_timeout", "check_timeout"}


def test_repo_env_file_has_working_example_values():
    text = (REPO / "pinger.env").read_text(encoding="utf-8")
    values = dict(line.split("=", 1) for line in text.splitlines()
                  if line.strip() and not line.startswith("#"))
    assert values["PING_INTERVAL_SECONDS"] == "300"
    assert values["DB_USER"] == "app_user"
    assert values["DB_PASSWORD"]
    assert values["LOG_FILE"]


# ---------- отдельный процесс с подменённым драйвером ----------

class Service:
    """Пингер отдельным процессом: строки stdout и stderr с моментом получения."""

    def __init__(self, tmp_path, pinger_dir=PINGER_DIR, mode="ok", interval=1, env=None):
        self.started = time.monotonic()
        self.out, self.err = [], []
        run_env = {k: v for k, v in os.environ.items() if not k.startswith("PG")}
        run_env.update({
            "DB_USER": "app_user", "DB_PASSWORD": "ordinary password",
            "PING_INTERVAL_SECONDS": str(interval), "FAKE_DRIVER": mode,
            "FAKE_DRIVER_STATE": str(tmp_path / "first_fail.marker"),
            "PYTHONDONTWRITEBYTECODE": "1", "PYTHONIOENCODING": "utf-8",
        })
        run_env.pop("LOG_FILE", None)
        run_env.update(env or {})
        self.proc = subprocess.Popen(
            [sys.executable, str(RUNNER), str(pinger_dir)], cwd=tmp_path, env=run_env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8",
        )
        self.threads = [threading.Thread(target=self._read, args=(self.proc.stdout, self.out), daemon=True),
                        threading.Thread(target=self._read, args=(self.proc.stderr, self.err), daemon=True)]
        for t in self.threads:
            t.start()

    @staticmethod
    def _read(stream, into):
        for line in stream:
            into.append((time.monotonic(), line.rstrip("\n")))

    def lines(self, where, pattern=""):
        return [line for _, line in (self.out if where == "out" else self.err) if pattern in line]

    def times(self, where, pattern=""):
        return [t for t, line in (self.out if where == "out" else self.err) if pattern in line]

    def wait_for(self, where, pattern, count=1, timeout=15):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if len(self.lines(where, pattern)) >= count:
                return
            if self.proc.poll() is not None:
                time.sleep(0.2)
                break
            time.sleep(0.05)
        if len(self.lines(where, pattern)) >= count:
            return
        self.kill()
        pytest.fail(f"не дождались {count} × {pattern!r} в {where} за {timeout} с\n"
                    f"stdout: {self.lines('out')}\nstderr: {self.lines('err')}")

    def stop(self, sig=signal.SIGTERM, timeout=10):
        """Посылает сигнал, возвращает (код выхода, секунд до выхода)."""
        sent = time.monotonic()
        self.proc.send_signal(sig)
        try:
            rc = self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.kill()
            pytest.fail(f"пингер не завершился за {timeout} с после {sig.name}")
        elapsed = time.monotonic() - sent
        for t in self.threads:
            t.join(timeout=5)
        return rc, elapsed

    def finish(self, timeout=10):
        rc = self.proc.wait(timeout=timeout)
        for t in self.threads:
            t.join(timeout=5)
        return rc

    def kill(self):
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=5)


@pytest.fixture
def service(tmp_path):
    running = []

    def start(**kwargs):
        s = Service(tmp_path, **kwargs)
        running.append(s)
        return s

    yield start
    for s in running:
        s.kill()


def test_success_is_written_only_to_stdout(service):
    s = service(mode="ok", interval=1)
    s.wait_for("out", "PostgreSQL 18.6", count=2)
    rc, _ = s.stop()
    assert rc == 0
    assert s.lines("err") == []
    assert all(" INFO " in line for line in s.lines("out", "PostgreSQL 18.6"))


def test_failure_is_written_only_to_stderr_as_error(service):
    s = service(mode="fail", interval=1)
    s.wait_for("out", "пингер запущен")
    s.wait_for("err", "Connection refused", count=2)
    first_error = s.times("err", "Connection refused")[0]
    assert first_error - s.started < 3
    rc, _ = s.stop()
    assert rc == 0
    assert all(" ERROR " in line for line in s.lines("err"))
    assert s.lines("out", "Connection refused") == []
    assert s.lines("out", " ERROR ") == []
    assert "Traceback" not in "\n".join(s.lines("err"))


def test_atypical_answer_is_warning_in_stdout_and_loop_continues(service):
    s = service(mode="atypical", interval=1)
    s.wait_for("out", "PostgreSQL 17.5", count=2)
    rc, _ = s.stop()
    assert rc == 0
    assert all(" WARNING " in line for line in s.lines("out", "PostgreSQL 17.5"))
    assert s.lines("err") == []


def test_first_failure_then_success_in_the_same_process(service):
    s = service(mode="first_fail", interval=1)
    s.wait_for("out", "PostgreSQL 18.6", count=2)
    rc, _ = s.stop()
    assert rc == 0
    assert len(s.lines("err", " ERROR ")) == 1
    assert s.times("err", " ERROR ")[0] < s.times("out", "PostgreSQL 18.6")[0]
    assert s.lines("out", " ERROR ") == []


def test_log_file_duplicates_both_streams(service, tmp_path):
    log_file = tmp_path / "pinger.log"
    s = service(mode="first_fail", interval=1, env={"LOG_FILE": str(log_file)})
    s.wait_for("out", "PostgreSQL 18.6", count=2)
    s.stop()
    logged = log_file.read_text(encoding="utf-8").splitlines()
    streamed = s.lines("out") + s.lines("err")
    assert streamed
    assert set(streamed) <= set(logged)
    assert any(" ERROR " in line for line in logged)
    assert any(" INFO " in line and "PostgreSQL 18.6" in line for line in logged)


def test_no_log_file_without_variable(service, tmp_path):
    s = service(mode="ok", interval=1)
    s.wait_for("out", "PostgreSQL 18.6")
    s.stop()
    assert [p.name for p in tmp_path.iterdir()] == []


def test_unwritable_log_file_is_reported_without_traceback(service):
    s = service(mode="ok", interval=1, env={"LOG_FILE": "/nonexistent/dir/pinger.log"})
    rc = s.finish()
    assert rc == 1
    assert s.lines("out") == []
    err = "\n".join(s.lines("err"))
    assert err and "Traceback" not in err
    assert "/nonexistent/dir/pinger.log" in err


@pytest.mark.parametrize("interval", [1, 2])
def test_interval_comes_from_environment(service, interval):
    s = service(mode="ok", interval=interval)
    s.wait_for("out", "PostgreSQL 18.6", count=3, timeout=15)
    s.stop()
    stamps = s.times("out", "PostgreSQL 18.6")[:3]
    steps = [b - a for a, b in zip(stamps, stamps[1:])]
    assert all(abs(step - interval) < 0.5 for step in steps), steps


@pytest.mark.parametrize("mode", ["hang", "hang_query"])
@pytest.mark.parametrize("check_timeout,interval,limit", [(2, 5, 2), (None, 2, 1.6)],
                         ids=["check_timeout-2", "default-below-interval"])
def test_silent_server_does_not_hang_the_service(service, tmp_path, mode, check_timeout, interval, limit):
    config = dict(FILE_CFG, connect_timeout=60)
    if check_timeout is not None:
        config["check_timeout"] = check_timeout
    s = service(pinger_dir=pinger_copy(tmp_path, config), mode=mode, interval=interval)
    s.wait_for("out", "пингер запущен")
    started = s.times("out", "пингер запущен")[0]
    s.wait_for("err", "не ответила", count=2, timeout=limit + interval + 6)
    first, second = s.times("err", "не ответила")[:2]
    assert first - started <= limit + 2, first - started
    assert abs((second - first) - interval) < 0.8, second - first
    rc, _ = s.stop()
    assert rc == 0
    assert all(" ERROR " in line for line in s.lines("err"))
    assert s.lines("out", "не ответила") == []


def test_sigterm_during_hung_check_stops_at_once(service, tmp_path):
    pinger_dir = pinger_copy(tmp_path, dict(FILE_CFG, connect_timeout=60, check_timeout=30))
    s = service(pinger_dir=pinger_dir, mode="hang", interval=100)
    s.wait_for("out", "пингер запущен")
    time.sleep(1)
    rc, elapsed = s.stop()
    assert rc == 0
    assert elapsed <= 2, elapsed
    assert s.lines("out", "получен сигнал SIGTERM")
    assert "Traceback" not in "\n".join(s.lines("err"))
    assert processes_mentioning(str(pinger_dir)) == []


def test_sigterm_between_checks_stops_at_once(service):
    s = service(mode="ok", interval=300)
    s.wait_for("out", "PostgreSQL 18.6")
    rc, elapsed = s.stop()
    assert rc == 0
    assert elapsed <= 2, elapsed
    assert s.lines("out", "получен сигнал SIGTERM")
    assert s.lines("err") == []


def test_config_path_cannot_be_changed_through_environment(service, tmp_path):
    evil = write_config(tmp_path, dict(FILE_CFG, host="evil.example"))
    driver_log = tmp_path / "driver.jsonl"
    s = service(mode="ok", interval=1, env={"PINGER_CONFIG": str(evil), "FAKE_DRIVER_LOG": str(driver_log)})
    s.wait_for("out", "PostgreSQL 18.6")
    s.stop()
    calls = [json.loads(line) for line in driver_log.read_text(encoding="utf-8").splitlines()]
    assert calls
    assert all(call["kwargs"]["host"] == "db" for call in calls)


def test_pg_environment_is_ignored(service, tmp_path):
    driver_log = tmp_path / "driver.jsonl"
    injected = {
        "PGHOST": "10.255.255.1", "PGPORT": "1", "PGDATABASE": "postgres",
        "PGOPTIONS": "-c work_mem=4GB", "PGPASSWORD": "environment password",
        "PGUSER": "postgres", "PGPASSFILE": "/etc/passwd", "PGSERVICE": "untrusted",
        "PGSSLMODE": "require",
    }
    s = service(mode="ok", interval=1, env={**injected, "FAKE_DRIVER_LOG": str(driver_log)})
    s.wait_for("out", "PostgreSQL 18.6")
    s.stop()
    calls = [json.loads(line) for line in driver_log.read_text(encoding="utf-8").splitlines()]
    assert calls
    for call in calls:
        assert call["pg_env"] == {}
        assert call["args"] == []
        assert {k: call["kwargs"][k] for k in FILE_CFG} == FILE_CFG
        assert call["kwargs"]["user"] == "app_user"
        assert call["kwargs"]["password"] == "ordinary password"
        assert set(call["kwargs"]) <= DRIVER_KEYS


BAD_START = [
    ("broken-brace", BROKEN_JSON[0], {}, "файл настроек повреждён"),
    ("broken-comma", BROKEN_JSON[1], {}, "файл настроек повреждён"),
    *[(f"extra-{key}", {**FILE_CFG, key: "injected"}, {}, f"недопустимые параметры в настройках: {key}")
      for key in EXTRA_KEYS],
    ("check_timeout-0", {**FILE_CFG, "check_timeout": 0}, {}, "check_timeout"),
    ("check_timeout-121", {**FILE_CFG, "check_timeout": 121}, {}, "check_timeout"),
    ("check_timeout-string", {**FILE_CFG, "check_timeout": "20"}, {}, "check_timeout"),
    ("interval-abc", FILE_CFG, {"PING_INTERVAL_SECONDS": "abc"}, "PING_INTERVAL_SECONDS"),
    ("interval-0", FILE_CFG, {"PING_INTERVAL_SECONDS": "0"}, "PING_INTERVAL_SECONDS"),
    ("interval-negative", FILE_CFG, {"PING_INTERVAL_SECONDS": "-5"}, "PING_INTERVAL_SECONDS"),
    ("interval-float", FILE_CFG, {"PING_INTERVAL_SECONDS": "5.5"}, "PING_INTERVAL_SECONDS"),
    ("interval-empty", FILE_CFG, {"PING_INTERVAL_SECONDS": ""}, "PING_INTERVAL_SECONDS"),
    ("no-user", FILE_CFG, {"DB_USER": None}, "DB_USER"),
    ("no-password", FILE_CFG, {"DB_PASSWORD": None}, "DB_PASSWORD"),
]


@pytest.mark.parametrize("config,env,message", [case[1:] for case in BAD_START],
                         ids=[case[0] for case in BAD_START])
def test_bad_settings_stop_start_with_clear_message(tmp_path, config, env, message):
    pinger_dir = pinger_copy(tmp_path, config)
    driver_log = tmp_path / "driver.jsonl"
    run_env = {k: v for k, v in os.environ.items() if not k.startswith("PG")}
    run_env.update({"DB_USER": "app_user", "DB_PASSWORD": "ordinary password",
                    "FAKE_DRIVER": "ok", "FAKE_DRIVER_LOG": str(driver_log),
                    "PYTHONDONTWRITEBYTECODE": "1", "PYTHONIOENCODING": "utf-8"})
    for key, value in env.items():
        if value is None:
            run_env.pop(key, None)
        else:
            run_env[key] = value
    result = subprocess.run([sys.executable, str(RUNNER), str(pinger_dir)], cwd=tmp_path, env=run_env,
                            capture_output=True, encoding="utf-8", timeout=20)
    assert result.returncode == 1
    assert result.stdout == ""
    assert "Ошибка настроек:" in result.stderr
    assert message in result.stderr
    assert "Traceback" not in result.stderr
    assert not driver_log.exists()
