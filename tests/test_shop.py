"""Проверки приложения магазина (практика 3).

Три группы:
- операции 6.1–6.4 с фейковым соединением: что ушло в текст запроса, а что
  параметрами; таблицы и колонки выбираются номером; списки значений и наборы
  строк уходят параметрами; связанные вставки идут одним запросом;
- main() в своём процессе с подменённым вводом и драйвером: успех в stdout,
  ошибки в stderr без текста драйвера и БД, полный текст в файле LOG_FILE,
  пароль нигде не печатается, настройки подключения и ввод логина/пароля;
- отдельный процесс через tests/fake_shop_driver.py: ввод пароля без
  терминала, недоступный файл лога, путь к настройкам, окружение.

Схема таблиц в фейковом соединении повторяет db/init/shop/10-schema.sql.
"""

import builtins
import datetime
import json
import logging
import os
import re
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

import psycopg
import pytest
from psycopg import errors, sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

REPO = Path(__file__).resolve().parents[1]
SHOP_DIR = REPO / "shop"
RUNNER = Path(__file__).with_name("fake_shop_driver.py")
sys.path.insert(0, str(SHOP_DIR))
import shop  # noqa: E402

# information_schema.columns для db/init/shop/10-schema.sql:
# таблица, колонка, тип, nullable, есть default, identity
COLUMNS = [
    ("authors", "id", "integer", False, False, True),
    ("authors", "full_name", "text", False, False, False),
    ("authors", "country", "text", True, False, False),
    ("books", "id", "integer", False, False, True),
    ("books", "author_id", "integer", False, False, False),
    ("books", "title", "text", False, False, False),
    ("books", "year", "integer", True, False, False),
    ("books", "price", "numeric", False, False, False),
    ("books", "stock", "integer", False, True, False),
    ("customers", "id", "integer", False, False, True),
    ("customers", "full_name", "text", False, False, False),
    ("customers", "email", "text", False, False, False),
    ("customers", "city", "text", True, False, False),
    ("order_items", "id", "integer", False, False, True),
    ("order_items", "order_id", "integer", False, False, False),
    ("order_items", "book_id", "integer", False, False, False),
    ("order_items", "count", "integer", False, False, False),
    ("order_items", "price", "numeric", False, False, False),
    ("orders", "id", "integer", False, False, True),
    ("orders", "customer_id", "integer", False, False, False),
    ("orders", "created_at", "date", False, True, False),
    ("orders", "status", "text", False, True, False),
]
# pg_constraint: дочерняя таблица, внешний ключ, родительская таблица
FOREIGN_KEYS = [
    ("books", "author_id", "authors"),
    ("order_items", "book_id", "books"),
    ("orders", "customer_id", "customers"),
    ("order_items", "order_id", "orders"),
]
TABLES = ["authors", "books", "customers", "order_items", "orders"]
# Номера в меню приложения
MENU = {"select_all": "1", "filter_one": "2", "filter_many": "3", "update_one": "4",
        "update_many": "5", "insert_one": "6", "insert_linked_one": "7",
        "insert_many": "8", "insert_linked_many": "9"}
TABLE_NO = {t: str(i) for i, t in enumerate(TABLES, 1)}

INJECTIONS = [
    "' OR 1=1 --",
    "1; DROP TABLE books; --",
    "%s",
    "{}",
    '"',
    "\\",
    "$1",
    "x' OR 'a'='a",
    "books\"; DROP TABLE books; --",
]
CFG = {"host": "db", "port": 5432, "dbname": "practice_db", "connect_timeout": 5}
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
DRIVER_KEYS = {"host", "port", "dbname", "connect_timeout", "user", "password",
               "passfile", "application_name"}
EXTRA_KEYS = ["user", "password", "options", "passfile", "sslmode", "search_path",
              "service", "statement_timeout", "sslrootcert"]
BROKEN_JSON = [json.dumps(CFG)[:-1], json.dumps(CFG).replace(",", "", 1)]
PG_ENV = {
    "PGHOST": "10.255.255.1", "PGPORT": "1", "PGDATABASE": "postgres",
    "PGOPTIONS": "-c work_mem=4GB", "PGPASSWORD": "environment password",
    "PGUSER": "postgres", "PGPASSFILE": "/etc/passwd", "PGSERVICE": "untrusted",
    "PGSSLMODE": "require",
}
PASSWORD = "typed secret pass"


# ---------- фейковое соединение ----------

class Result:
    def __init__(self, rows=(), columns=(), rowcount=None):
        self.rows = [tuple(r) for r in rows]
        self.columns = list(columns)
        self.rowcount = len(self.rows) if rowcount is None else rowcount


class ColumnInfo:
    def __init__(self, name):
        self.name = name


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.connection = conn
        self.description = None
        self.rowcount = -1
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, query, params=None):
        text = query if isinstance(query, str) else query.as_string(self.conn)
        if "information_schema.columns" in text:
            return self._set(Result(self.conn.columns))
        if "pg_constraint" in text:
            return self._set(Result(self.conn.fks))
        self.conn.calls.append((text, params))
        outcome = self.conn.outcomes.pop(0) if self.conn.outcomes else Result()
        if isinstance(outcome, BaseException):
            raise outcome
        return self._set(outcome)

    def _set(self, result):
        self._rows = list(result.rows)
        self.description = [ColumnInfo(n) for n in result.columns]
        self.rowcount = result.rowcount
        return self

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class Transaction:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class FakeConn:
    """Записывает текст и параметры каждого execute; метаданные отдаёт сам."""

    connection = None  # для sql.Composable.as_string: без кодировки сервера

    def __init__(self, outcomes=(), columns=COLUMNS, fks=FOREIGN_KEYS):
        self.calls = []
        self.outcomes = list(outcomes)
        self.columns = columns
        self.fks = fks
        self.autocommit = False
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self):
        return FakeCursor(self)

    def execute(self, query, params=None):
        return FakeCursor(self).execute(query, params)

    def transaction(self):
        return Transaction()

    @property
    def query(self):
        (text, _), = self.calls
        return text

    @property
    def params(self):
        (_, params), = self.calls
        return params


def flatten(value):
    if isinstance(value, (list, tuple)):
        return [x for v in value for x in flatten(v)]
    return [value]


class Console:
    """Ответы на вопросы программы по порядку; каждый вопрос записывается."""

    def __init__(self, monkeypatch, answers, password=PASSWORD):
        self.answers = list(answers)
        self.prompts = []
        self.password = password
        monkeypatch.setattr(builtins, "input", self.input)
        monkeypatch.setattr(shop.getpass, "getpass", self.getpass)

    def input(self, prompt=""):
        self.prompts.append(prompt)
        if not self.answers:
            raise AssertionError(f"программа задала лишний вопрос: {prompt!r}; "
                                 f"все вопросы: {self.prompts}")
        return self.answers.pop(0)

    def getpass(self, prompt=""):
        self.prompts.append(("getpass", prompt))
        return self.password


@pytest.fixture(autouse=True)
def isolated_shop(monkeypatch):
    # Окружение и обработчики логгеров возвращаются после каждого теста.
    monkeypatch.setattr(os, "environ", os.environ.copy())
    for key in list(os.environ):
        if key.startswith("PG"):
            del os.environ[key]
    monkeypatch.delenv("LOG_FILE", raising=False)
    saved = (list(shop.log.handlers), list(shop.raw.handlers), shop.raw.propagate)
    yield
    for handler in shop.log.handlers + shop.raw.handlers:
        if isinstance(handler, logging.FileHandler):
            handler.close()
    shop.log.handlers[:], shop.raw.handlers[:], shop.raw.propagate = saved


@pytest.fixture
def schema():
    return shop.Schema(FakeConn())


def run_main(monkeypatch, answers, outcomes=(), connect_error=None, password=PASSWORD):
    """main() с подменёнными вводом и драйвером. Возвращает (rc, conn, console, kwargs)."""
    console = Console(monkeypatch, ["app_user", *answers], password=password)
    conn = FakeConn(outcomes)
    calls = []

    def fake_connect(*args, **kwargs):
        calls.append((args, kwargs, {k: v for k, v in os.environ.items() if k.startswith("PG")}))
        if connect_error is not None:
            raise connect_error
        return conn

    monkeypatch.setattr(shop.psycopg, "connect", fake_connect)
    rc = shop.main()
    return rc, conn, console, calls


def numbered(text, title):
    """Пункты нумерованного списка, напечатанного после строки title."""
    block = text.split(title, 1)[1]
    items = []
    for line in block.splitlines()[1:]:
        m = re.match(r"\s+(\d+)\. (.*)$", line)
        if not m:
            break
        items.append(m.group(2))
    return items


# ---------- метаданные: белый список таблиц и колонок ----------

def test_schema_comes_from_database_metadata(schema):
    assert list(schema.tables) == TABLES
    assert [c.name for c in schema.columns("books")] == ["id", "author_id", "title", "year", "price", "stock"]
    assert schema.relations == [
        ("authors", "books", "author_id"), ("books", "order_items", "book_id"),
        ("customers", "orders", "customer_id"), ("orders", "order_items", "order_id"),
    ]


def test_tables_and_columns_are_chosen_by_number(monkeypatch, capsys, schema):
    # 6.1.2: и таблица, и колонка выбираются номером из списка по схеме
    console = Console(monkeypatch, [TABLE_NO["books"], "3", "Dune"])
    conn = FakeConn()
    shop.select_filtered(conn, schema, many=False)
    out = capsys.readouterr().out
    assert numbered(out, "Таблица:") == TABLES
    assert numbered(out, "Колонка для фильтра:") == [
        "id (integer)", "author_id (integer)", "title (text)", "year (integer)",
        "price (numeric)", "stock (integer)"]
    assert all(p.startswith("Выберите номер") or p.startswith("  title") for p in console.prompts)
    assert len(conn.calls) == 1


@pytest.mark.parametrize("bad", ["abc", "0", "6", "99", "-1", "1.5", "1;DROP TABLE books", "authors",
                                 '"authors"', "1 OR 1=1"])
def test_choice_not_from_list_is_asked_again_without_query(monkeypatch, capsys, schema, bad):
    console = Console(monkeypatch, [bad, TABLE_NO["books"]])
    conn = FakeConn()
    shop.select_all(conn, schema)
    out = capsys.readouterr().out
    assert out.count("Нет такого номера") == 1
    assert len(conn.calls) == 1
    assert '"books"' in conn.query
    assert "authors" not in conn.query
    assert [p for p in console.prompts if "Выберите" in p] == ["Выберите номер (пусто - назад): "] * 2


@pytest.mark.parametrize("bad", ["abc", "0,1", "1,abc", "1;2", "1,99", "title,year", "1, DROP"])
def test_many_choice_not_from_list_is_asked_again(monkeypatch, capsys, schema, bad):
    Console(monkeypatch, [TABLE_NO["books"], bad, "3,4", "Dune", "1965"])
    conn = FakeConn()
    shop.select_filtered(conn, schema, many=True)
    out = capsys.readouterr().out
    assert out.count("Нет такого номера") == 1
    assert len(conn.calls) == 1
    assert bad not in conn.query


# ---------- 6.1 просмотр ----------

@pytest.mark.parametrize("table", TABLES)
def test_select_all_has_no_filter_and_no_limit(monkeypatch, schema, table):
    # 6.1.1: SELECT * FROM table; — все строки, без WHERE и без LIMIT
    Console(monkeypatch, [TABLE_NO[table]])
    conn = FakeConn([Result([(1, "a"), (2, "b"), (3, "c")], ["id", "x"])])
    shop.select_all(conn, schema)
    assert re.fullmatch(rf'SELECT \* FROM "{table}"( ORDER BY 1)?', conn.query), conn.query
    assert not conn.params


def test_select_all_prints_every_row(monkeypatch, capsys, schema):
    rows = [(i, f"name {i}") for i in range(1, 251)]
    Console(monkeypatch, [TABLE_NO["authors"]])
    conn = FakeConn([Result(rows, ["id", "full_name"])])
    shop.select_all(conn, schema)
    out = capsys.readouterr().out
    assert all(f"name {i}" in out for i in range(1, 251))
    assert "показаны первые" not in out


@pytest.mark.parametrize("value", ["Dune", *INJECTIONS])
def test_filter_one_sends_column_as_identifier_and_value_as_parameter(monkeypatch, schema, value):
    # 6.1.2: SELECT * FROM table WHERE @id = $id — колонка по выбору, значение параметром
    Console(monkeypatch, [TABLE_NO["books"], "3", value])
    conn = FakeConn()
    shop.select_filtered(conn, schema, many=False)
    assert re.fullmatch(r'SELECT \* FROM "books" WHERE "title" = %s::text( ORDER BY 1)?', conn.query), conn.query
    assert tuple(conn.params) == (value,)
    assert conn.query.count("%s") == 1


def test_filter_one_by_another_column(monkeypatch, schema):
    Console(monkeypatch, [TABLE_NO["customers"], "4", "Москва"])
    conn = FakeConn()
    shop.select_filtered(conn, schema, many=False)
    assert re.fullmatch(r'SELECT \* FROM "customers" WHERE "city" = %s::text( ORDER BY 1)?', conn.query)
    assert tuple(conn.params) == ("Москва",)


@pytest.mark.parametrize("bad", ["abc", "1 OR 1=1", "1; DROP TABLE books", "1.5", "1965-01-01"])
def test_wrong_value_type_is_asked_again_before_query(monkeypatch, capsys, schema, bad):
    Console(monkeypatch, [TABLE_NO["books"], "4", bad, "1965"])
    conn = FakeConn()
    shop.select_filtered(conn, schema, many=False)
    out = capsys.readouterr().out
    assert "ожидается целое число" in out
    assert len(conn.calls) == 1
    assert list(conn.params)[0] == 1965
    assert bad not in conn.query


def test_filter_many_joins_conditions_with_and(monkeypatch, schema):
    # 6.1.3: SELECT * FROM table WHERE @id = $id AND @value = $value
    Console(monkeypatch, [TABLE_NO["books"], "3,4", "Dune", "1965"])
    conn = FakeConn()
    shop.select_filtered(conn, schema, many=True)
    assert re.fullmatch(
        r'SELECT \* FROM "books" WHERE "title" = %s::text AND "year" = %s::int4( ORDER BY 1)?',
        conn.query), conn.query
    assert tuple(conn.params) == ("Dune", 1965)


def test_filter_many_three_conditions(monkeypatch, schema):
    Console(monkeypatch, [TABLE_NO["order_items"], "2,3,4", "1", "2", "3"])
    conn = FakeConn()
    shop.select_filtered(conn, schema, many=True)
    assert re.fullmatch(
        r'SELECT \* FROM "order_items" WHERE "order_id" = %s::int4 AND "book_id" = %s::int4'
        r' AND "count" = %s::int4( ORDER BY 1)?', conn.query), conn.query
    assert tuple(conn.params) == (1, 2, 3)


def test_filter_many_requires_at_least_two_columns(monkeypatch, capsys, schema):
    # одна колонка — это 6.1.2; здесь вопрос повторяется, пока не выбрано несколько
    Console(monkeypatch, [TABLE_NO["books"], "3", "3,4", "Dune", "1965"])
    conn = FakeConn()
    shop.select_filtered(conn, schema, many=True)
    assert len(conn.calls) == 1
    assert " AND " in conn.query, conn.query
    assert tuple(conn.params) == ("Dune", 1965)


# ---------- 6.2 обновление ----------

def test_update_one_changes_chosen_columns_of_one_record(monkeypatch, capsys, schema):
    # 6.2.1: UPDATE SET @value = $value, @cost = $cost WHERE id = $id
    Console(monkeypatch, [TABLE_NO["books"], "1", "2,4", "New title", "12.50"])
    conn = FakeConn([Result(rowcount=1)])
    shop.update_one(conn, schema)
    assert re.fullmatch(
        r'UPDATE "books" SET "title" = %s::text, "price" = %s::numeric WHERE "?id"? = %s(::int4)?',
        conn.query), conn.query
    assert tuple(conn.params) == ("New title", Decimal("12.50"), 1)
    out = capsys.readouterr().out
    assert numbered(out, "Какие поля изменить:") == [
        "author_id (integer)", "title (text)", "year (integer)", "price (numeric)", "stock (integer)"]


@pytest.mark.parametrize("bad", ["0", "6", "1,6", "id"])
def test_update_one_cannot_pick_id_column(monkeypatch, capsys, schema, bad):
    Console(monkeypatch, [TABLE_NO["books"], "1", bad, "2", "New title"])
    conn = FakeConn([Result(rowcount=1)])
    shop.update_one(conn, schema)
    assert capsys.readouterr().out.count("Нет такого номера") == 1
    assert len(conn.calls) == 1
    assert 'SET "title" = %s::text WHERE' in conn.query
    assert 'SET "id"' not in conn.query


@pytest.mark.parametrize("value", INJECTIONS)
def test_update_one_values_are_parameters(monkeypatch, schema, value):
    Console(monkeypatch, [TABLE_NO["authors"], "7", "1,2", value, value])
    conn = FakeConn([Result(rowcount=1)])
    shop.update_one(conn, schema)
    assert tuple(conn.params) == (value, value, 7)
    assert conn.query.count("%s") == 3
    assert re.fullmatch(r'UPDATE "authors" SET "full_name" = %s::text, "country" = %s::text WHERE .*', conn.query)


@pytest.mark.parametrize("values", [[3], [1, 2], [5, 6, 7, 8, 9]], ids=["one", "two", "five"])
def test_update_many_uses_in_with_one_placeholder_per_value(monkeypatch, schema, values):
    # 6.2.2: UPDATE SET @cost = $cost WHERE @value IN ($value1, $value2, …)
    Console(monkeypatch, [TABLE_NO["books"], "5", "0", "2", *map(str, values), ""])
    conn = FakeConn([Result(rowcount=len(values))])
    shop.update_many(conn, schema)
    placeholders = ", ".join(["%s::int4"] * len(values))
    assert conn.query == f'UPDATE "books" SET "stock" = %s::int4 WHERE "author_id" IN ({placeholders})', conn.query
    assert list(conn.params) == [0, *values]


@pytest.mark.parametrize("values", [INJECTIONS, ["Dune", "' OR 1=1 --"]], ids=["injections", "mixed"])
def test_update_many_text_values_stay_parameters(monkeypatch, schema, values):
    Console(monkeypatch, [TABLE_NO["books"], "5", "0", "3", *values, ""])
    conn = FakeConn([Result(rowcount=0)])
    shop.update_many(conn, schema)
    assert list(conn.params) == [0, *values]
    assert conn.query.count("%s") == len(values) + 1
    for v in values:
        if v != "%s":
            assert v not in conn.query
    assert re.fullmatch(r'UPDATE "books" SET "stock" = %s::int4 WHERE "title" IN \((%s::text, )*%s::text\)', conn.query)


def test_update_many_reports_rows_count(monkeypatch, capsys, schema):
    Console(monkeypatch, [TABLE_NO["books"], "5", "0", "2", "1", "2", "3", ""])
    conn = FakeConn([Result(rowcount=4)])
    handler = logging.StreamHandler(sys.stdout)
    shop.log.addHandler(handler)
    shop.log.setLevel(logging.INFO)
    shop.update_many(conn, schema)
    assert "Обновлено записей в таблице books: 4" in capsys.readouterr().out


# ---------- 6.3 вставка одной строки ----------

def test_insert_one_values_are_parameters_and_id_is_not_asked(monkeypatch, schema):
    # 6.3.1: INSERT INTO (…) VALUES ($…); id генерирует база
    console = Console(monkeypatch, [TABLE_NO["authors"], "1", "Фрэнк Герберт", "' OR 1=1 --"])
    conn = FakeConn([Result([(7,)], ["id"])])
    shop.insert_one(conn, schema)
    assert conn.query == 'INSERT INTO "authors" ("full_name", "country") VALUES (%s::text, %s::text) RETURNING id'
    assert tuple(conn.params) == ("Фрэнк Герберт", "' OR 1=1 --")
    assert not any(p.startswith("  id ") for p in console.prompts)


def test_insert_one_required_columns_only(monkeypatch, schema):
    Console(monkeypatch, [TABLE_NO["books"], "", "1", "Dune", "12.50"])
    conn = FakeConn([Result([(42,)], ["id"])])
    shop.insert_one(conn, schema)
    assert conn.query == ('INSERT INTO "books" ("author_id", "title", "price") '
                          'VALUES (%s::int4, %s::text, %s::numeric) RETURNING id')
    assert tuple(conn.params) == (1, "Dune", Decimal("12.50"))


def test_insert_linked_one_takes_foreign_key_from_parent_insert(monkeypatch, schema):
    # 6.3.2: id родителя из RETURNING идёт во вставку дочерней строки
    # связь 4 (orders -> order_items); необязательные поля orders не заполняем;
    # customer_id; у order_items все поля обязательные: book_id, count, price
    console = Console(monkeypatch, ["4", "", "1", "2", "3", "10.50"])
    conn = FakeConn([Result([(5, 9)], ["order_id", "id"])])
    shop.insert_linked_one(conn, schema)
    assert len(conn.calls) == 1
    q = " ".join(conn.query.split())
    assert 'INSERT INTO "orders" ("customer_id") VALUES (%s::int4) RETURNING id' in q
    assert 'INSERT INTO "order_items" ("order_id", "book_id", "count", "price")' in q
    assert q.index('INSERT INTO "orders"') < q.index('INSERT INTO "order_items"')
    assert tuple(conn.params) == (1, 2, 3, Decimal("10.50"))
    assert not any("order_id" in str(p) or p.startswith("  id ") for p in console.prompts)


def test_insert_linked_one_lists_relations_by_foreign_keys(monkeypatch, capsys, schema):
    # authors -> books: без необязательных полей автора; имя; без необязательных
    # полей книги (year, stock); title, price
    Console(monkeypatch, ["1", "", "Автор", "", "Книга", "5"])
    conn = FakeConn([Result([(1, 2)], ["author_id", "id"])])
    shop.insert_linked_one(conn, schema)
    assert numbered(capsys.readouterr().out, "Связанные таблицы:") == [
        "authors -> books (по author_id)", "books -> order_items (по book_id)",
        "customers -> orders (по customer_id)", "orders -> order_items (по order_id)"]
    assert tuple(conn.params) == ("Автор", "Книга", Decimal("5"))


# ---------- 6.4 вставка нескольких строк ----------

@pytest.mark.parametrize("names", [["Автор 1", "Автор 2"], ["A", "B", "C"], INJECTIONS],
                         ids=["two", "three", "injections"])
def test_insert_many_sends_all_rows_as_parameters_in_one_query(monkeypatch, schema, names):
    # 6.4.1: несколько строк одним действием, значения только параметрами
    answers = [TABLE_NO["authors"], ""]
    for i, name in enumerate(names):
        answers += [name, "да" if i < len(names) - 1 else ""]
    Console(monkeypatch, answers)
    conn = FakeConn([Result([(i,) for i in range(10, 10 + len(names))], ["id"])])
    shop.insert_many(conn, schema)
    assert len(conn.calls) == 1
    assert conn.query.startswith('INSERT INTO "authors" ("full_name")')
    assert flatten(conn.params) == names
    for name in names:
        if len(name) > 2 and name != "%s":
            assert name not in conn.query


def test_insert_many_two_columns(monkeypatch, schema):
    Console(monkeypatch, [TABLE_NO["authors"], "1", "A", "Россия", "да", "B", "Франция", ""])
    conn = FakeConn([Result([(1,), (2,)], ["id"])])
    shop.insert_many(conn, schema)
    assert len(conn.calls) == 1
    assert sorted(flatten(conn.params)) == sorted(["A", "Россия", "B", "Франция"])


@pytest.mark.parametrize("children_per_parent", [[1, 3], [2, 1, 4], [1]], ids=["1+3", "2+1+4", "single"])
def test_insert_linked_many_is_one_query_with_rows_bound_by_number(monkeypatch, schema, children_per_parent):
    # 6.4.2 (повышенная сложность): несколько родителей с дочерними строками одним
    # запросом; id родителей генерирует база, в программу не возвращаются
    answers = ["4"]  # orders -> order_items
    answers += [""]  # необязательные поля orders не заполняем
    expected = []
    for n, count in enumerate(children_per_parent, 1):
        answers.append(str(n))  # customer_id
        expected.append(n)
        for k in range(1, count + 1):
            answers += [str(10 * n + k), str(k), "1.00"]  # book_id, count, price
            expected += [10 * n + k, k, Decimal("1.00")]
            answers.append("да" if k < count else "")
        answers.append("да" if n < len(children_per_parent) else "")
    console = Console(monkeypatch, answers)
    conn = FakeConn([Result([([101, 102, 103][:len(children_per_parent)], sum(children_per_parent))])])
    shop.insert_linked_many(conn, schema)
    assert len(conn.calls) == 1
    q = " ".join(conn.query.split())
    assert 'INSERT INTO "orders"' in q and 'INSERT INTO "order_items"' in q
    assert q.index('INSERT INTO "orders"') < q.index('INSERT INTO "order_items"')
    assert not any("order_id" in str(p) or p.startswith("  id ") for p in console.prompts)
    values = flatten(conn.params)
    for v in expected:
        assert v in values
    # номер родителя для каждой дочерней строки передан параметром
    numbers = [n for n, count in enumerate(children_per_parent, 1) for _ in range(count)]
    assert numbers in [list(p) for p in conn.params if isinstance(p, list)]
    assert len(children_per_parent) in flatten(conn.params)


# ---------- main(): потоки вывода, ошибки, файл лога ----------

DB_DETAILS = ["psycopg", "Error", "Violation", "SQLSTATE", "ERROR:", "DETAIL:", "ОШИБКА",
              "ПОДРОБНОСТИ", "Traceback", "_fkey", "_check", "_key", "FATAL", "Warning"]


def assert_no_db_details(text, exc=None):
    for fragment in DB_DETAILS:
        assert fragment not in text, (fragment, text)
    if exc is not None:
        assert type(exc).__name__ not in text
        assert str(exc).splitlines()[0] not in text
        if getattr(exc, "sqlstate", None):
            assert exc.sqlstate not in text


def test_success_goes_to_stdout_and_nothing_to_stderr(monkeypatch, capsys):
    rc, conn, console, calls = run_main(
        monkeypatch, [MENU["select_all"], TABLE_NO["books"], ""],
        outcomes=[Result([(1, 1, "Dune", 1965, Decimal("12.50"), 3)],
                         ["id", "author_id", "title", "year", "price", "stock"])])
    assert rc == 0
    out, err = capsys.readouterr()
    assert err == ""
    assert "Подключение к базе practice_db выполнено" in out
    assert "Запрос выполнен" in out
    assert "Dune" in out
    assert "Выход" in out
    assert console.prompts[:2] == ["Логин: ", ("getpass", "Пароль: ")]
    assert PASSWORD not in out


@pytest.mark.parametrize("error", [
    psycopg.OperationalError('connection failed: FATAL:  password authentication failed for user "app_user"'),
    psycopg.OperationalError('connection failed: connection to server at "172.18.0.2", port 5432 failed: '
                             'Connection refused'),
    psycopg.OperationalError("connection timeout expired"),
], ids=["auth", "refused", "timeout"])
def test_connection_failure_goes_to_stderr_without_details(monkeypatch, capsys, error):
    rc, conn, console, calls = run_main(monkeypatch, [], connect_error=error)
    assert rc != 0
    out, err = capsys.readouterr()
    assert out == ""
    assert err == ("Ошибка: не удалось подключиться к базе данных: проверьте логин, пароль "
                   "и доступность сервера\n")
    assert_no_db_details(err, error)
    assert conn.calls == []


@pytest.mark.parametrize("password", ["wrong", *ATTACKS])
def test_connection_failure_never_prints_password(monkeypatch, capsys, tmp_path, password):
    log_file = tmp_path / "shop.log"
    monkeypatch.setenv("LOG_FILE", str(log_file))
    error = psycopg.OperationalError(f'connection failed: FATAL:  password authentication failed; "{password}"')
    rc, conn, console, calls = run_main(monkeypatch, [], connect_error=error, password=password)
    assert rc != 0
    out, err = capsys.readouterr()
    assert password not in out and password not in err
    (args, kwargs, pg_env), = calls
    assert kwargs["password"] == password


QUERY_ERRORS = [
    ("fk", [MENU["insert_one"], TABLE_NO["books"], "", "999", "Dune", "12.50"],
     errors.ForeignKeyViolation('INSERT или UPDATE в таблице "books" нарушает ограничение внешнего ключа '
                                '"books_author_id_fkey"\nDETAIL:  Ключ (author_id)=(999) отсутствует в таблице "authors".'),
     "ссылка на несуществующую запись"),
    ("unique", [MENU["insert_one"], TABLE_NO["customers"], "", "Иван", "ivan@example.com"],
     errors.UniqueViolation('повторяющееся значение ключа нарушает ограничение уникальности '
                            '"customers_email_key"\nDETAIL:  Ключ "(email)=(ivan@example.com)" уже существует.'),
     "такая запись уже есть"),
    ("check", [MENU["insert_one"], TABLE_NO["order_items"], "1", "1", "0", "5"],
     errors.CheckViolation('новая строка в отношении "order_items" нарушает ограничение-проверку '
                           '"order_items_count_check"\nDETAIL:  Ошибочная строка содержит (6, 1, 1, 0, 5.00).'),
     "не прошло проверку"),
    ("privilege", [MENU["update_one"], TABLE_NO["books"], "1", "2", "X"],
     errors.InsufficientPrivilege('нет доступа к таблице books'),
     "недостаточно прав"),
    ("lost", [MENU["select_all"], TABLE_NO["books"]],
     psycopg.OperationalError("server closed the connection unexpectedly\n\tThis probably means the server "
                              "terminated abnormally"),
     "потеряно соединение"),
    ("update-fk", [MENU["update_many"], TABLE_NO["books"], "1", "999", "2", "1", ""],
     errors.ForeignKeyViolation('INSERT или UPDATE в таблице "books" нарушает ограничение внешнего ключа '
                                '"books_author_id_fkey"'),
     "ссылка на несуществующую запись"),
]


@pytest.mark.parametrize("with_log_file", [True, False], ids=["log-file", "no-log-file"])
@pytest.mark.parametrize("answers,error,friendly", [c[1:] for c in QUERY_ERRORS], ids=[c[0] for c in QUERY_ERRORS])
def test_query_failure_goes_to_stderr_friendly_and_full_text_to_log_file(
        monkeypatch, capsys, tmp_path, answers, error, friendly, with_log_file):
    # 6.5: пользователю — понятный текст в stderr; полный текст драйвера и БД —
    # только в файл из 6.7, и ни в один поток, задана переменная или нет
    log_file = tmp_path / "shop.log"
    if with_log_file:
        monkeypatch.setenv("LOG_FILE", str(log_file))
    rc, conn, console, calls = run_main(monkeypatch, [*answers, ""], outcomes=[error])
    assert rc == 0, "после ошибки запроса программа возвращается в меню"
    out, err = capsys.readouterr()
    assert len(conn.calls) == 1
    error_lines = [line for line in err.splitlines() if line.strip()]
    assert len(error_lines) == 1, err
    assert error_lines[0].startswith("Ошибка: ")
    assert friendly in error_lines[0]
    assert_no_db_details(err, error)
    assert_no_db_details(out, error)
    assert friendly not in out
    assert "Подключение к базе practice_db выполнено" in out
    assert "Выход" in out
    if not with_log_file:
        assert not log_file.exists()
        return

    logged = log_file.read_text(encoding="utf-8")
    assert type(error).__name__ in logged
    assert str(error).splitlines()[0] in logged
    for line in out.splitlines():
        if line.startswith(("Подключение", "Запрос", "Выход", "В таблицу", "Запись", "Обновлено", "Добавлено")):
            assert line in logged, line
    assert error_lines[0].removeprefix("Ошибка: ") in logged
    assert PASSWORD not in logged


def test_lost_connection_ends_program_with_error_code(monkeypatch, capsys):
    error = psycopg.OperationalError("server closed the connection unexpectedly")
    conn_holder = {}

    class ClosingConn(FakeConn):
        def cursor(self):
            cur = super().cursor()
            original = cur.execute

            def execute(query, params=None):
                try:
                    return original(query, params)
                except psycopg.OperationalError:
                    self.closed = True
                    raise
            cur.execute = execute
            return cur

    conn_holder["conn"] = ClosingConn([error])
    monkeypatch.setattr(shop.psycopg, "connect", lambda **kwargs: conn_holder["conn"])
    Console(monkeypatch, ["app_user", MENU["select_all"], TABLE_NO["books"]])
    rc = shop.main()
    assert rc == 2
    out, err = capsys.readouterr()
    assert "потеряно соединение" in err
    assert_no_db_details(err, error)


def test_no_log_file_without_variable(monkeypatch, capsys, tmp_path):
    monkeypatch.chdir(tmp_path)
    rc, conn, console, calls = run_main(monkeypatch, [MENU["select_all"], TABLE_NO["books"], ""])
    assert rc == 0
    assert list(tmp_path.iterdir()) == []
    assert "Запрос выполнен" in capsys.readouterr().out


def test_log_file_duplicates_success_lines(monkeypatch, capsys, tmp_path):
    log_file = tmp_path / "shop.log"
    monkeypatch.setenv("LOG_FILE", str(log_file))
    rc, conn, console, calls = run_main(
        monkeypatch, [MENU["insert_one"], TABLE_NO["authors"], "", "Автор", ""],
        outcomes=[Result([(7,)], ["id"])])
    assert rc == 0
    out, err = capsys.readouterr()
    assert err == ""
    assert "В таблицу authors добавлена запись id=7" in out
    logged = log_file.read_text(encoding="utf-8")
    assert "Подключение к базе practice_db выполнено" in logged
    assert "В таблицу authors добавлена запись id=7" in logged
    assert "Выход" in logged
    assert PASSWORD not in logged


@pytest.mark.parametrize("user,password", [("", "x"), ("app_user", ""), ("   ", "x")],
                         ids=["no-login", "no-password", "blank-login"])
def test_empty_login_or_password_is_rejected_before_connect(monkeypatch, capsys, user, password):
    calls = []
    monkeypatch.setattr(shop.psycopg, "connect", lambda **kwargs: calls.append(kwargs))
    Console(monkeypatch, [user], password=password)
    assert shop.main() != 0
    assert calls == []
    out, err = capsys.readouterr()
    assert out == ""
    assert "Traceback" not in err and err.count("Ошибка:") == 1


# ---------- подключение: настройки и ввод (наследие практик 1–2) ----------

@pytest.mark.parametrize("field", ["user", "password"])
@pytest.mark.parametrize("attack", ATTACKS)
def test_connect_keeps_credential_in_its_field(monkeypatch, field, attack):
    credentials = {"user": "app_user", "password": "ordinary password", field: attack}
    calls = []
    monkeypatch.setattr(shop.psycopg, "connect", lambda *a, **kw: calls.append((a, kw)) or FakeConn())
    cfg = dict(CFG)
    shop.connect(cfg, credentials["user"], credentials["password"])
    (args, kwargs), = calls
    assert args == ()
    assert kwargs["user"] == credentials["user"]
    assert kwargs["password"] == credentials["password"]
    assert {k: kwargs[k] for k in CFG} == CFG
    assert kwargs["passfile"] == os.devnull
    assert set(kwargs) <= DRIVER_KEYS
    assert cfg == CFG
    # Настоящий драйвер собирает строку подключения и разбирает её обратно:
    # ввод остаётся в своём поле, новых опций не появляется.
    parsed = conninfo_to_dict(make_conninfo(**kwargs))
    assert parsed == {k: str(v) for k, v in kwargs.items()}


def test_connect_ignores_pg_environment(monkeypatch):
    for key, value in PG_ENV.items():
        monkeypatch.setenv(key, value)
    calls = []

    def capture(*args, **kwargs):
        calls.append((args, kwargs, {k: v for k, v in os.environ.items() if k.startswith("PG")}))
        return FakeConn()

    monkeypatch.setattr(shop.psycopg, "connect", capture)
    shop.connect(dict(CFG), "app_user", "typed password")
    (args, kwargs, pg_env), = calls
    assert pg_env == {}
    assert {k: kwargs[k] for k in CFG} == CFG
    assert set(kwargs) <= DRIVER_KEYS


def write_config(directory, data):
    path = Path(directory) / "config.json"
    path.write_text(data if isinstance(data, str) else json.dumps(data), encoding="utf-8")
    return path


@pytest.mark.parametrize("extra", EXTRA_KEYS)
def test_config_rejects_credentials_and_extra_options(tmp_path, extra):
    with pytest.raises(shop.ConfigError, match=f"недопустимые параметры.*{extra}"):
        shop.load_config(write_config(tmp_path, {**CFG, extra: "injected"}))


@pytest.mark.parametrize("content", BROKEN_JSON, ids=["missing-brace", "missing-comma"])
def test_config_reports_broken_json(tmp_path, content):
    with pytest.raises(shop.ConfigError, match="файл настроек повреждён"):
        shop.load_config(write_config(tmp_path, content))


BAD_CONFIGS = [
    ("missing-brace", BROKEN_JSON[0], "файл настроек повреждён"),
    ("missing-comma", BROKEN_JSON[1], "файл настроек повреждён"),
    ("port-string", {**CFG, "port": "5432"}, "port должен быть числом"),
    ("host-injection", {**CFG, "host": "db host=evil"}, "неверное значение host"),
    ("dbname-injection", {**CFG, "dbname": "x' options='-c"}, "неверное значение dbname"),
    *[(f"extra-{key}", {**CFG, key: "injected"}, f"недопустимые параметры в настройках: {key}")
      for key in EXTRA_KEYS],
    ("missing-host", {k: v for k, v in CFG.items() if k != "host"}, "не хватает параметров: host"),
    ("not-object", "[1, 2]", "ожидается объект JSON"),
]


@pytest.mark.parametrize("data,message", [c[1:] for c in BAD_CONFIGS], ids=[c[0] for c in BAD_CONFIGS])
def test_main_reports_config_error_without_asking_or_connecting(tmp_path, monkeypatch, capsys, data, message):
    path = write_config(tmp_path, data)
    load_config = shop.load_config
    monkeypatch.setattr(shop, "load_config", lambda: load_config(path))

    def unexpected(*args, **kwargs):
        pytest.fail("при ошибке настроек ввод и подключение не должны начинаться")

    monkeypatch.setattr(builtins, "input", unexpected)
    monkeypatch.setattr(shop.getpass, "getpass", unexpected)
    monkeypatch.setattr(shop.psycopg, "connect", unexpected)
    assert shop.main() == 1
    out, err = capsys.readouterr()
    assert out == ""
    assert err.startswith("Ошибка: настройки: ")
    assert message in err
    assert "Traceback" not in err


def test_main_uses_host_port_dbname_from_config_file(monkeypatch, capsys):
    rc, conn, console, calls = run_main(monkeypatch, [""])
    assert rc == 0
    (args, kwargs, pg_env), = calls
    file_cfg = json.loads((SHOP_DIR / "config.json").read_text(encoding="utf-8"))
    assert {k: kwargs[k] for k in ("host", "port", "dbname")} == {k: file_cfg[k] for k in ("host", "port", "dbname")}
    assert kwargs["user"] == "app_user" and kwargs["password"] == PASSWORD


# ---------- файлы репозитория: настройки, env-файл, compose, образ ----------

def test_repo_config_has_no_credentials():
    data = json.loads((SHOP_DIR / "config.json").read_text(encoding="utf-8"))
    assert "user" not in data and "password" not in data
    assert set(data) <= {"host", "port", "dbname", "connect_timeout"}
    assert data["host"] == "db" and data["port"] == 5432


def env_values(path):
    text = path.read_text(encoding="utf-8")
    return dict(line.split("=", 1) for line in text.splitlines() if line.strip() and not line.startswith("#"))


def test_repo_env_file_has_only_log_path():
    # 6.10: env-файл содержит работающий пример переменной из 6.7, без логина и пароля
    values = env_values(REPO / "shop.env")
    assert set(values) == {"LOG_FILE"}
    assert values["LOG_FILE"].startswith("/")


def compose_service(name):
    text = (REPO / "compose.yaml").read_text(encoding="utf-8")
    block = re.search(rf"^  {name}:\n((?:    .*\n|\n)*)", text, re.M)
    assert block, f"в compose.yaml нет сервиса {name}"
    return block.group(1)


def test_compose_shop_service_uses_tagged_image_env_file_and_interactive_mode():
    block = compose_service("shop")
    image = re.search(r"^    image:\s*(\S+)", block, re.M).group(1)
    assert ":" in image and not image.endswith(":latest"), image
    assert re.search(r"^    env_file:\s*\./shop\.env", block, re.M)
    assert re.search(r"^    stdin_open:\s*true", block, re.M)
    assert re.search(r"^    tty:\s*true", block, re.M)
    assert "ports:" not in block


def test_compose_db_is_postgres_18_with_volume():
    block = compose_service("db")
    assert re.search(r"image:\s*\S*postgres\S*:18", block), block
    assert re.search(r"^      - pgdata:/var/lib/postgresql", block, re.M)


def test_image_contains_code_config_and_pinned_dependencies():
    dockerfile = (SHOP_DIR / "Dockerfile").read_text(encoding="utf-8")
    assert re.search(r"^COPY .*\bshop\.py\b.*\bconfig\.json\b", dockerfile, re.M)
    requirements = [line for line in (SHOP_DIR / "requirements.txt").read_text().splitlines() if line.strip()]
    assert requirements and all("==" in line for line in requirements)


def test_dotenv_is_ignored_by_git():
    assert ".env" in (REPO / ".gitignore").read_text(encoding="utf-8").splitlines()


# ---------- отдельный процесс с подменённым драйвером ----------

def run_shop(tmp_path, stdin="app_user\n" + PASSWORD + "\n", env=None, shop_dir=SHOP_DIR):
    run_env = {k: v for k, v in os.environ.items() if not k.startswith("PG")}
    run_env.pop("LOG_FILE", None)
    run_env.pop("SHOP_CONFIG", None)
    run_env.update({"PYTHONDONTWRITEBYTECODE": "1", "PYTHONIOENCODING": "utf-8"})
    run_env.update(env or {})
    # start_new_session: без управляющего терминала, как в контейнере без tty
    return subprocess.run([sys.executable, str(RUNNER), str(shop_dir)], cwd=tmp_path, env=run_env,
                          input=stdin, capture_output=True, encoding="utf-8", timeout=30,
                          start_new_session=True)


def test_password_without_terminal_is_read_without_python_warnings(tmp_path):
    result = run_shop(tmp_path)
    assert result.returncode == 2
    assert "не удалось подключиться к базе данных" in result.stderr
    for fragment in ["Warning", "GetPassWarning", "echoed", ".py:", "Traceback"]:
        assert fragment not in result.stderr, result.stderr
    assert PASSWORD not in result.stdout and PASSWORD not in result.stderr
    assert "Ошибка" not in result.stdout


def test_unwritable_log_file_is_reported_without_traceback(tmp_path):
    result = run_shop(tmp_path, env={"LOG_FILE": "/nonexistent/dir/shop.log"})
    assert result.returncode == 1
    assert result.stdout == ""
    assert "Traceback" not in result.stderr
    assert "/nonexistent/dir/shop.log" in result.stderr
    assert result.stderr.startswith("Ошибка: ")


def test_log_file_gets_full_connection_error_text(tmp_path):
    log_file = tmp_path / "shop.log"
    result = run_shop(tmp_path, env={"LOG_FILE": str(log_file)})
    assert result.returncode == 2
    logged = log_file.read_text(encoding="utf-8")
    assert "OperationalError" in logged
    assert "password authentication failed" in logged
    assert "OperationalError" not in result.stderr
    assert "password authentication failed" not in result.stderr
    assert PASSWORD not in logged
    assert "не удалось подключиться к базе данных" in logged


def test_config_path_cannot_be_changed_through_environment(tmp_path):
    evil = write_config(tmp_path, dict(CFG, host="evil.example", port=1))
    driver_log = tmp_path / "driver.jsonl"
    result = run_shop(tmp_path, env={"SHOP_CONFIG": str(evil), "FAKE_DRIVER_LOG": str(driver_log)})
    assert result.returncode == 2
    (call,), = [[json.loads(line) for line in driver_log.read_text(encoding="utf-8").splitlines()]]
    assert call["kwargs"]["host"] == "db"
    assert call["kwargs"]["port"] == 5432


def test_pg_environment_is_ignored_in_process(tmp_path):
    driver_log = tmp_path / "driver.jsonl"
    result = run_shop(tmp_path, env={**PG_ENV, "FAKE_DRIVER_LOG": str(driver_log)})
    assert result.returncode == 2
    (call,), = [[json.loads(line) for line in driver_log.read_text(encoding="utf-8").splitlines()]]
    assert call["pg_env"] == {}
    assert call["args"] == []
    assert {k: call["kwargs"][k] for k in ("host", "port", "dbname")} == {"host": "db", "port": 5432, "dbname": "practice_db"}
    assert call["kwargs"]["user"] == "app_user" and call["kwargs"]["password"] == PASSWORD
    assert set(call["kwargs"]) <= DRIVER_KEYS


# ---------- разбор значений и сборка запросов ----------

def col(dtype, name="c", nullable=False):
    return shop.Column(name, dtype, nullable, False, False)


@pytest.mark.parametrize("dtype,text,expected", [
    ("integer", "42", 42),
    ("integer", "-7", -7),
    ("numeric", "199,90", Decimal("199.90")),
    ("numeric", "0.5", Decimal("0.5")),
    ("date", "2026-09-28", datetime.date(2026, 9, 28)),
    ("boolean", "да", True),
    ("text", "' OR 1=1; DROP TABLE books; --", "' OR 1=1; DROP TABLE books; --"),
    ("text", "", None),
])
def test_parse_ok(dtype, text, expected):
    assert shop.parse_value(col(dtype), text) == expected


@pytest.mark.parametrize("dtype,text", [
    ("integer", "1 OR 1=1"),
    ("integer", "1; DROP TABLE books"),
    ("integer", "1.5"),
    ("numeric", "NaN"),
    ("numeric", "abc"),
    ("date", "28.09.2026"),
    ("boolean", "может быть"),
    ("text", "a\x00b"),
])
def test_parse_rejected(dtype, text):
    with pytest.raises(ValueError):
        shop.parse_value(col(dtype), text)


def test_unknown_type_not_supported():
    # колонки с типами не из словаря TYPES приложение не показывает
    assert col("jsonb").cast is None


def test_identifier_is_quoted():
    # даже если бы в белый список попало странное имя, оно осталось бы именем
    q = sql.SQL("SELECT * FROM {} WHERE {} = %s").format(
        sql.Identifier("books"), sql.Identifier('id" = 1 OR "1'))
    assert q.as_string(None) == 'SELECT * FROM "books" WHERE "id"" = 1 OR ""1" = %s'


def test_placeholder_cast_comes_from_types_table():
    assert shop.ph(col("integer")).as_string(None) == "%s::int4"
    assert shop.ph_array(col("numeric")).as_string(None) == "%s::numeric[]"
