import logging
import sys
from pathlib import Path

import psycopg
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pinger"))
import pinger  # noqa: E402

CFG = {"host": "db", "port": 5432, "dbname": "practice_db", "connect_timeout": 5}


class FakeConn:
    def __init__(self, answer):
        self.answer = answer

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, _q):
        return self

    def fetchone(self):
        return self.answer


@pytest.fixture
def records(monkeypatch):
    got = []

    class H(logging.Handler):
        def emit(self, record):
            got.append(record)

    h = H()
    pinger.log.addHandler(h)
    pinger.log.setLevel(logging.INFO)
    yield got
    pinger.log.removeHandler(h)


def test_normal_version(monkeypatch, records):
    v = "PostgreSQL 18.6 (Debian 18.6-1.pgdg12+2) on x86_64-pc-linux-gnu, compiled by gcc"
    monkeypatch.setattr(pinger.psycopg, "connect", lambda **kw: FakeConn((v,)))
    pinger.check_once(CFG, "u", "p")
    assert records[-1].levelno == logging.INFO


@pytest.mark.parametrize("answer", [("MySQL 8.0",), (None,), ("",), None, (42,)])
def test_atypical_answer_is_warning(monkeypatch, records, answer):
    monkeypatch.setattr(pinger.psycopg, "connect", lambda **kw: FakeConn(answer))
    pinger.check_once(CFG, "u", "p")
    assert records[-1].levelno == logging.WARNING


def test_connection_error_is_error(monkeypatch, records):
    def boom(**kw):
        raise psycopg.OperationalError("connection timeout expired")
    monkeypatch.setattr(pinger.psycopg, "connect", boom)
    pinger.check_once(CFG, "u", "p")
    assert records[-1].levelno == logging.ERROR


def test_timeouts_passed_to_driver(monkeypatch):
    seen = {}

    def capture(**kw):
        seen.update(kw)
        return FakeConn(("PostgreSQL 18.6 on x86_64",))
    monkeypatch.setattr(pinger.psycopg, "connect", capture)
    pinger.check_once(CFG, "u", "p")
    assert seen["connect_timeout"] == 5
    assert seen["keepalives"] == 1
    assert seen["tcp_user_timeout"] == 5000


@pytest.mark.parametrize("env,ok", [
    ({"PING_INTERVAL_SECONDS": "300"}, True),
    ({}, True),
    ({"PING_INTERVAL_SECONDS": "5m"}, False),
    ({"PING_INTERVAL_SECONDS": "0"}, False),
    ({"PING_INTERVAL_SECONDS": "-1"}, False),
])
def test_interval_env(monkeypatch, env, ok):
    monkeypatch.delenv("PING_INTERVAL_SECONDS", raising=False)
    monkeypatch.setenv("DB_USER", "u")
    monkeypatch.setenv("DB_PASSWORD", "p")
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    if ok:
        assert pinger.load_env()[0] == int(env.get("PING_INTERVAL_SECONDS", 300))
    else:
        with pytest.raises(pinger.ConfigError):
            pinger.load_env()


def test_credentials_required(monkeypatch):
    monkeypatch.delenv("DB_USER", raising=False)
    monkeypatch.setenv("DB_PASSWORD", "p")
    with pytest.raises(pinger.ConfigError):
        pinger.load_env()
