import datetime
import sys
from decimal import Decimal
from pathlib import Path

import pytest
from psycopg import sql

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "shop"))
import shop  # noqa: E402


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
