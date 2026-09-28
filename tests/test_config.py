import json
import sys
from pathlib import Path

import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
from db_connect import ConfigError, load_config  # noqa: E402

GOOD = {"host": "127.0.0.1", "port": 55432, "dbname": "practice_db", "connect_timeout": 5}


def write(tmp_path, content):
    p = tmp_path / "config.json"
    p.write_text(content if isinstance(content, str) else json.dumps(content), encoding="utf-8")
    return p


def test_good_config(tmp_path):
    assert load_config(write(tmp_path, GOOD)) == GOOD


@pytest.mark.parametrize("extra", ["options", "passfile", "sslmode", "work_mem", "statement_timeout"])
def test_extra_key_rejected(tmp_path, extra):
    with pytest.raises(ConfigError, match=extra):
        load_config(write(tmp_path, {**GOOD, extra: "x"}))


def test_broken_json(tmp_path):
    # убрали закрывающую скобку
    with pytest.raises(ConfigError, match="повреждён"):
        load_config(write(tmp_path, json.dumps(GOOD)[:-1]))


def test_missing_file(tmp_path):
    with pytest.raises(ConfigError, match="не найден"):
        load_config(tmp_path / "nope.json")


@pytest.mark.parametrize("field,value", [
    ("port", "55432"), ("port", 0), ("port", True), ("port", 70000),
    ("host", "db host=evil"), ("host", ""), ("dbname", "x' options='-c"),
    ("connect_timeout", 0), ("connect_timeout", 1000),
])
def test_bad_values(tmp_path, field, value):
    with pytest.raises(ConfigError):
        load_config(write(tmp_path, {**GOOD, field: value}))


def test_not_object(tmp_path):
    with pytest.raises(ConfigError):
        load_config(write(tmp_path, "[1, 2]"))


@pytest.mark.parametrize("password", [
    "x' host=evil.example options='-c work_mem=4GB",
    "pass host=evil",
    "a\\' dbname=postgres",
])
def test_password_cannot_inject_params(password):
    # то же, что делает psycopg.connect(**kwargs) внутри
    conninfo = make_conninfo(host="127.0.0.1", port=55432, dbname="practice_db",
                             user="app_user", password=password)
    parsed = conninfo_to_dict(conninfo)
    assert parsed["password"] == password
    assert parsed["host"] == "127.0.0.1"
    assert parsed["dbname"] == "practice_db"
    assert "options" not in parsed
