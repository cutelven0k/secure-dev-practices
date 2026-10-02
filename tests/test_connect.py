import builtins
import json
import os
import sys
from pathlib import Path

import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
import db_connect  # noqa: E402

GOOD = {"host": "127.0.0.1", "port": 55432, "dbname": "practice_db", "connect_timeout": 5}
ATTACKS = [
    "x host=evil",
    "x' host=evil options='-c work_mem=4GB",
    "x' passfile='/etc/passwd",
    "x' sslrootcert='/tmp/x",
    "options=-c work_mem=4GB",
    "host=evil host=other",
    "a'\"\\b",
    "two words",
    "  password with edge spaces  ",
    "a&b?c=postgresql://evil",
    "postgresql://evil/other?host=evil&options=-c%20work_mem=4GB",
]
EXTRA_KEYS = [
    "user", "password", "options", "service", "passfile", "sslrootcert",
    "search_path", "target_session_attrs", "statement_timeout", "work_mem",
]


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch):
    # connect очищает PG*: исходное окружение остальных тестов сохраняется.
    monkeypatch.setattr(os, "environ", os.environ.copy())


def write_config(tmp_path, data):
    path = tmp_path / "config.json"
    path.write_text(data if isinstance(data, str) else json.dumps(data), encoding="utf-8")
    return path


def expected_params(cfg, user, password):
    return {**cfg, "user": user, "password": password,
            "passfile": "/dev/null", "application_name": "practice1"}


@pytest.mark.parametrize("field", ["user", "password"])
@pytest.mark.parametrize("attack", ATTACKS)
def test_connect_keeps_credential_in_its_field(tmp_path, monkeypatch, field, attack):
    cfg = db_connect.load_config(write_config(tmp_path, GOOD))
    credentials = {"user": "app_user", "password": "ordinary password", field: attack}
    calls = []
    connection = object()

    def capture_connect(*args, **kwargs):
        calls.append((args, kwargs))
        return connection

    monkeypatch.setattr(db_connect.psycopg, "connect", capture_connect)
    assert db_connect.connect(cfg, **credentials) is connection
    assert calls == [((), expected_params(GOOD, **credentials))]
    assert cfg == GOOD


@pytest.mark.parametrize("field", ["user", "password"])
@pytest.mark.parametrize("attack", ATTACKS)
def test_driver_roundtrip_preserves_credentials(field, attack):
    credentials = {"user": "app_user", "password": "ordinary password", field: attack}
    params = expected_params(GOOD, **credentials)
    parsed = conninfo_to_dict(make_conninfo(**params))
    assert parsed == {key: str(value) for key, value in params.items()}


def test_connect_ignores_pg_environment(tmp_path, monkeypatch):
    injected = {
        "PGHOST": "10.255.255.1", "PGPORT": "1", "PGDATABASE": "postgres",
        "PGOPTIONS": "-c work_mem=4GB", "PGPASSWORD": "environment password",
        "PGUSER": "postgres", "PGPASSFILE": "/etc/passwd",
        "PGSERVICE": "untrusted", "PGSSLROOTCERT": "/tmp/x",
    }
    for key, value in injected.items():
        monkeypatch.setenv(key, value)
    calls = []

    def capture_connect(*args, **kwargs):
        # Проверка в момент вызова: libpq мог бы прочитать оставшиеся PG*.
        calls.append((args, kwargs, {key: os.environ[key] for key in injected if key in os.environ}))

    monkeypatch.setattr(db_connect.psycopg, "connect", capture_connect)
    cfg = db_connect.load_config(write_config(tmp_path, GOOD))
    db_connect.connect(cfg, "app_user", "typed password")
    assert calls == [((), expected_params(GOOD, "app_user", "typed password"), {})]


@pytest.mark.parametrize("extra", EXTRA_KEYS)
def test_config_rejects_credentials_and_extra_options(tmp_path, extra):
    with pytest.raises(db_connect.ConfigError, match=f"недопустимые параметры.*{extra}"):
        db_connect.load_config(write_config(tmp_path, {**GOOD, extra: "injected"}))


@pytest.mark.parametrize("value", [0, 61, "5", True])
def test_config_rejects_invalid_connect_timeout(tmp_path, value):
    with pytest.raises(db_connect.ConfigError, match="connect_timeout должен быть числом от 1 до 60"):
        db_connect.load_config(write_config(tmp_path, {**GOOD, "connect_timeout": value}))


@pytest.mark.parametrize("missing", ["host", "port", "dbname"])
def test_config_names_missing_required_key(tmp_path, missing):
    cfg = {key: value for key, value in GOOD.items() if key != missing}
    with pytest.raises(db_connect.ConfigError, match=f"не хватает параметров: {missing}"):
        db_connect.load_config(write_config(tmp_path, cfg))


def test_connect_works_without_optional_timeout(tmp_path, monkeypatch):
    cfg = db_connect.load_config(write_config(tmp_path, {key: value for key, value in GOOD.items()
                                                         if key != "connect_timeout"}))
    calls = []
    monkeypatch.setattr(db_connect.psycopg, "connect", lambda **kwargs: calls.append(kwargs))
    db_connect.connect(cfg, "app_user", "typed password")
    assert calls == [expected_params(GOOD, "app_user", "typed password")]


BROKEN_JSON = [json.dumps(GOOD)[:-1], json.dumps(GOOD).replace(",", "", 1)]


@pytest.mark.parametrize("content", BROKEN_JSON, ids=["missing-brace", "missing-comma"])
def test_config_reports_broken_json(tmp_path, content):
    with pytest.raises(db_connect.ConfigError, match="файл настроек повреждён"):
        db_connect.load_config(write_config(tmp_path, content))


MAIN_CONFIG_ERRORS = [
    (BROKEN_JSON[0], "файл настроек повреждён"),
    (BROKEN_JSON[1], "файл настроек повреждён"),
    ({**GOOD, "port": "55432"}, "port должен быть числом от 1 до 65535"),
    ({**GOOD, "port": 0}, "port должен быть числом от 1 до 65535"),
    ({**GOOD, "host": "db host=evil"}, "неверное значение host"),
    *[({**GOOD, extra: "injected"}, f"недопустимые параметры в настройках: {extra}")
      for extra in EXTRA_KEYS],
    *[({**GOOD, "connect_timeout": value}, "connect_timeout должен быть числом от 1 до 60")
      for value in [0, 61, "5", True]],
    *[({key: value for key, value in GOOD.items() if key != missing},
       f"не хватает параметров: {missing}") for missing in ["host", "port", "dbname"]],
]
MAIN_CONFIG_ERROR_IDS = [
    "missing-brace", "missing-comma", "port-string", "port-zero", "host-injection",
    *[f"extra-{key}" for key in EXTRA_KEYS],
    "timeout-zero", "timeout-61", "timeout-string", "timeout-bool",
    "missing-host", "missing-port", "missing-dbname",
]


@pytest.mark.parametrize("data,message", MAIN_CONFIG_ERRORS, ids=MAIN_CONFIG_ERROR_IDS)
def test_main_reports_config_error_without_connecting(tmp_path, monkeypatch, capsys, data, message):
    path = write_config(tmp_path, data)
    load_config = db_connect.load_config
    monkeypatch.setattr(db_connect, "load_config", lambda: load_config(path))

    def unexpected_call(*args, **kwargs):
        pytest.fail("При ошибке настроек ввод и подключение не должны начинаться")

    monkeypatch.setattr(builtins, "input", unexpected_call)
    monkeypatch.setattr(db_connect.getpass, "getpass", unexpected_call)
    monkeypatch.setattr(db_connect.psycopg, "connect", unexpected_call)
    assert db_connect.main() != 0
    output = capsys.readouterr()
    assert output.out == ""
    assert "Ошибка настроек:" in output.err
    assert message in output.err
    assert "Traceback" not in output.err


def enter_credentials(monkeypatch, user="app_user", password="typed password"):
    prompts = []

    def read_user(prompt):
        prompts.append(("input", prompt))
        return user

    def read_password(prompt):
        prompts.append(("getpass", prompt))
        return password

    monkeypatch.setattr(builtins, "input", read_user)
    monkeypatch.setattr(db_connect.getpass, "getpass", read_password)
    return prompts


@pytest.mark.parametrize("password", ["wrong", *ATTACKS])
def test_main_reports_connection_failure_without_details(monkeypatch, capsys, password):
    enter_credentials(monkeypatch, password=password)
    calls = []

    def fail_connect(*args, **kwargs):
        calls.append((args, kwargs))
        raise db_connect.psycopg.OperationalError("password authentication failed; /etc/passwd")

    monkeypatch.setattr(db_connect.psycopg, "connect", fail_connect)
    cfg = json.loads(db_connect.CONFIG_PATH.read_text(encoding="utf-8"))
    assert db_connect.main() != 0
    assert calls == [((), expected_params(cfg, "app_user", password))]
    output = capsys.readouterr()
    assert output.out == ""
    assert output.err == ("Не удалось подключиться к базе данных: проверьте логин, пароль "
                          "и доступность сервера\n")
    assert "Traceback" not in output.err


def test_main_reads_nearby_config_prompts_and_prints_version(monkeypatch, capsys):
    # Настоящий load_config читает config.json рядом с программой.
    cfg = json.loads(db_connect.CONFIG_PATH.read_text(encoding="utf-8"))
    monkeypatch.setenv("PGHOST", "10.255.255.1")
    monkeypatch.setenv("PGUSER", "postgres")
    monkeypatch.setenv("PGPASSWORD", "environment password")
    password = "  x' passfile='/etc/passwd host=evil  "
    prompts = enter_credentials(monkeypatch, user="app_user", password=password)
    calls, queries = [], []
    version = "PostgreSQL 18.6"

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, query):
            queries.append(query)
            return self

        def fetchone(self):
            return (version,)

    def capture_connect(*args, **kwargs):
        calls.append((args, kwargs))
        return Connection()

    monkeypatch.setattr(db_connect.psycopg, "connect", capture_connect)
    assert db_connect.main() == 0
    assert calls == [((), expected_params(cfg, "app_user", password))]
    assert prompts == [("input", "Логин: "), ("getpass", "Пароль: ")]
    assert queries == ["SELECT VERSION();"]
    output = capsys.readouterr()
    assert output.err == ""
    assert output.out == f"Подключение успешно. Версия сервера: {version}\n"
    assert password not in output.out
