"""Практика 3: консольное приложение для работы с базой книжного магазина.

Таблицы и колонки пользователь выбирает номерами из списка, который
строится по метаданным базы. Имена колонок попадают в SQL только через
sql.Identifier, значения только через параметры запроса.
"""

import datetime
import getpass
import json
import logging
import os
import re
import sys
import warnings
from decimal import Decimal, InvalidOperation
from pathlib import Path

import psycopg
from psycopg import errors, sql

# Файл настроек лежит рядом с программой, путь через окружение не меняется
CONFIG_PATH = Path(__file__).with_name("config.json")

ALLOWED_KEYS = {"host", "port", "dbname", "connect_timeout"}
REQUIRED_KEYS = {"host", "port", "dbname"}
HOST_RE = re.compile(r"^[A-Za-z0-9.\-]{1,253}$")
DBNAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")

# Поддерживаемые типы колонок: тип из information_schema -> (приведение в SQL,
# функция разбора ввода). Приведение берётся только из этого словаря.
TYPES = {
    "integer": "int4",
    "bigint": "int8",
    "smallint": "int2",
    "numeric": "numeric",
    "text": "text",
    "character varying": "varchar",
    "date": "date",
    "boolean": "bool",
}

# Сообщения пользователю: что пошло не так, без текста ошибки от БД
FRIENDLY = [
    (errors.UniqueViolation, "такая запись уже есть (повтор уникального значения)"),
    (errors.ForeignKeyViolation, "ссылка на несуществующую запись в связанной таблице"),
    (errors.NotNullViolation, "не заполнено обязательное поле"),
    (errors.CheckViolation, "значение не прошло проверку ограничений таблицы"),
    (errors.InsufficientPrivilege, "недостаточно прав для этой операции"),
    (errors.NumericValueOutOfRange, "число вне допустимого диапазона"),
    (errors.DataError, "неверный формат данных"),
    (errors.GeneratedAlways, "это поле заполняется автоматически"),
    (psycopg.OperationalError, "потеряно соединение с базой данных"),
]

# out/err печатают пользователю и дублируют в файл лога.
# raw пишет только в файл: полный текст ошибок драйвера и БД.
log = logging.getLogger("shop")
raw = logging.getLogger("shop.raw")


class ConfigError(Exception):
    pass


class InputCancelled(Exception):
    """Пользователь ввёл пустую строку там, где можно вернуться в меню."""


# ---------- настройки и подключение ----------

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


def setup_logging():
    class MaxLevel(logging.Filter):
        def filter(self, record):
            return record.levelno < logging.ERROR

    out = logging.StreamHandler(sys.stdout)
    out.addFilter(MaxLevel())
    out.setFormatter(logging.Formatter("%(message)s"))
    err = logging.StreamHandler(sys.stderr)
    err.setLevel(logging.ERROR)
    err.setFormatter(logging.Formatter("Ошибка: %(message)s"))

    log.setLevel(logging.INFO)
    log.addHandler(out)
    log.addHandler(err)
    # raw не передаёт записи родителю (propagate=False), иначе полный текст
    # ошибок БД попал бы в консоль пользователю через обработчики "shop"
    raw.setLevel(logging.INFO)
    raw.propagate = False
    # без обработчиков logging отдал бы записи raw в stderr (lastResort)
    raw.addHandler(logging.NullHandler())

    log_file = os.environ.get("LOG_FILE", "").strip()
    if log_file:
        try:
            fh = logging.FileHandler(log_file, encoding="utf-8")
        except OSError:
            raise ConfigError(f"не удалось открыть файл логов {log_file}") from None
        fh.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s"))
        log.addHandler(fh)
        raw.addHandler(fh)


def clear_pg_env():
    for key in list(os.environ):
        if key.startswith("PG"):
            del os.environ[key]


def connect(cfg, user, password):
    clear_pg_env()
    conn = psycopg.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname=cfg["dbname"],
        connect_timeout=cfg["connect_timeout"],
        user=user,
        password=password,
        passfile=os.devnull,
        application_name="shop",
    )
    # каждая операция идёт в своей транзакции через conn.transaction()
    conn.autocommit = True
    return conn


def report_db_error(e, action):
    for cls, text in FRIENDLY:
        if isinstance(e, cls):
            msg = text
            break
    else:
        msg = "не удалось выполнить запрос"
    log.error("%s: %s", action, msg)
    raw.error("%s: %s: %s", action, type(e).__name__, str(e).strip())


# ---------- метаданные: белый список таблиц и колонок ----------

class Column:
    def __init__(self, name, data_type, nullable, has_default, identity):
        self.name = name
        self.cast = TYPES.get(data_type)
        self.data_type = data_type
        self.nullable = nullable
        self.has_default = has_default
        self.identity = identity

    @property
    def required(self):
        return not self.identity and not self.nullable and not self.has_default

    def __str__(self):
        return f"{self.name} ({self.data_type})"


class Schema:
    def __init__(self, conn):
        # information_schema.columns показывает только колонки, к которым
        # у текущего пользователя есть доступ, так что список уже с учётом прав
        rows = conn.execute("""
            SELECT c.table_name, c.column_name, c.data_type,
                   c.is_nullable = 'YES', c.column_default IS NOT NULL,
                   c.is_identity = 'YES'
            FROM information_schema.columns c
            JOIN information_schema.tables t
              ON t.table_schema = c.table_schema AND t.table_name = c.table_name
            WHERE c.table_schema = 'public' AND t.table_type = 'BASE TABLE'
            ORDER BY c.table_name, c.ordinal_position
        """).fetchall()
        self.tables = {}
        for table, name, dtype, nullable, has_default, identity in rows:
            col = Column(name, dtype, nullable, has_default, identity)
            if col.cast:
                self.tables.setdefault(table, []).append(col)

        # внешние ключи вида child.fk -> parent.id, из pg_catalog,
        # потому что information_schema показывает ограничения только владельцу
        fks = conn.execute("""
            SELECT ch.relname, a.attname, par.relname
            FROM pg_constraint con
            JOIN pg_class ch  ON ch.oid = con.conrelid
            JOIN pg_class par ON par.oid = con.confrelid
            JOIN pg_namespace n ON n.oid = ch.relnamespace
            JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = con.conkey[1]
            JOIN pg_attribute pa ON pa.attrelid = con.confrelid AND pa.attnum = con.confkey[1]
            WHERE con.contype = 'f' AND n.nspname = 'public'
              AND cardinality(con.conkey) = 1 AND pa.attname = 'id'
            ORDER BY par.relname, ch.relname
        """).fetchall()
        self.relations = [
            (parent, child, fk) for child, fk, parent in fks
            if parent in self.tables and child in self.tables
        ]

    def columns(self, table):
        return self.tables[table]

    def column(self, table, name):
        for c in self.tables[table]:
            if c.name == name:
                return c
        raise KeyError(name)


# ---------- ввод ----------

def ask(prompt):
    try:
        return input(prompt)
    except EOFError:
        raise InputCancelled from None


def choose(title, options, allow_many=False, min_count=1):
    """Выбор из списка по номеру. Возвращает элемент(ы) списка, а не ввод."""
    print(title)
    for i, opt in enumerate(options, 1):
        print(f"  {i}. {opt}")
    hint = "номера через запятую" if allow_many else "номер"
    while True:
        s = ask(f"Выберите {hint} (пусто - назад): ").strip()
        if not s:
            raise InputCancelled
        parts = [p.strip() for p in s.split(",")] if allow_many else [s]
        if not all(p.isdigit() and 1 <= int(p) <= len(options) for p in parts):
            print("  Нет такого номера, попробуйте ещё раз")
            continue
        picked = []
        for p in parts:
            item = options[int(p) - 1]
            if item not in picked:
                picked.append(item)
        if len(picked) < min_count:
            print(f"  Нужно выбрать не меньше {min_count} разных номеров")
            continue
        return picked if allow_many else picked[0]


def parse_value(col, text):
    """Перевод строки в значение нужного типа. None для пустого ввода."""
    if text == "":
        return None
    t = col.cast
    if t in ("int2", "int4", "int8"):
        if not re.fullmatch(r"-?\d{1,18}", text):
            raise ValueError("ожидается целое число")
        return int(text)
    if t == "numeric":
        try:
            v = Decimal(text.replace(",", "."))
        except InvalidOperation:
            raise ValueError("ожидается число, например 199.90") from None
        if not v.is_finite():
            raise ValueError("ожидается конечное число")
        return v
    if t == "date":
        try:
            return datetime.date.fromisoformat(text)
        except ValueError:
            raise ValueError("ожидается дата в формате ГГГГ-ММ-ДД") from None
    if t == "bool":
        low = text.lower()
        if low in ("да", "yes", "true", "1"):
            return True
        if low in ("нет", "no", "false", "0"):
            return False
        raise ValueError("ожидается да/нет")
    if "\x00" in text:
        raise ValueError("недопустимый символ")
    return text


def ask_value(col, allow_empty=False):
    while True:
        text = ask(f"  {col.name} ({col.data_type}): ").strip()
        if text == "" and not allow_empty:
            print("  Значение обязательно")
            continue
        try:
            return parse_value(col, text)
        except ValueError as e:
            print(f"  {e}")


def ask_values_list(col):
    print(f"Значения для {col.name}, по одному в строке, пустая строка - конец:")
    values = []
    while True:
        text = ask("  > ").strip()
        if text == "":
            if values:
                return values
            print("  Нужно хотя бы одно значение")
            continue
        try:
            values.append(parse_value(col, text))
        except ValueError as e:
            print(f"  {e}")


def ph(col):
    """Плейсхолдер с приведением типа: %s::int4. Тип берётся из словаря TYPES."""
    return sql.SQL("%s::{}").format(sql.SQL(col.cast))


def ph_array(col):
    return sql.SQL("%s::{}[]").format(sql.SQL(col.cast))


def insert_columns(schema, table, exclude=()):
    """Какие колонки заполнять: обязательные всегда, необязательные по выбору."""
    cols = [c for c in schema.columns(table) if not c.identity and c.name not in exclude]
    required = [c for c in cols if c.required]
    optional = [c for c in cols if not c.required]
    chosen = []
    if optional:
        print(f"Обязательные поля {table}: " + ", ".join(c.name for c in required))
        try:
            chosen = choose("Какие необязательные поля заполнить (пусто - никакие)?",
                            optional, allow_many=True)
        except InputCancelled:
            chosen = []
    result = [c for c in cols if c in required or c in chosen]
    if not result:
        print("Нужно выбрать хотя бы одно поле")
        raise InputCancelled
    return result


# ---------- вывод ----------

def print_rows(cur):
    rows = cur.fetchall()
    if not rows:
        log.info("Запрос выполнен, строк не найдено")
        return
    names = [d.name for d in cur.description]
    data = [["" if v is None else str(v) for v in r] for r in rows]
    widths = [min(40, max(len(n), *(len(r[i]) for r in data))) for i, n in enumerate(names)]
    line = " | ".join(n.ljust(w) for n, w in zip(names, widths))
    print(line)
    print("-" * len(line))
    for r in data:
        print(" | ".join(v[:w].ljust(w) for v, w in zip(r, widths)))
    log.info("Запрос выполнен, строк: %d", len(rows))


# ---------- операции ----------

def select_all(conn, schema):
    table = choose("Таблица:", list(schema.tables))
    q = sql.SQL("SELECT * FROM {} ORDER BY 1").format(sql.Identifier(table))
    with conn.cursor() as cur:
        cur.execute(q)
        print_rows(cur)


def select_filtered(conn, schema, many):
    table = choose("Таблица:", list(schema.tables))
    cols = schema.columns(table)
    if many:
        picked = choose("Колонки для фильтра (не меньше двух):", cols, allow_many=True, min_count=2)
    else:
        picked = [choose("Колонка для фильтра:", cols)]
    print("Значения для фильтра:")
    values = [ask_value(c) for c in picked]
    where = sql.SQL(" AND ").join(
        sql.SQL("{} = {}").format(sql.Identifier(c.name), ph(c)) for c in picked
    )
    q = sql.SQL("SELECT * FROM {} WHERE {} ORDER BY 1").format(sql.Identifier(table), where)
    with conn.cursor() as cur:
        cur.execute(q, values)
        print_rows(cur)


def update_one(conn, schema):
    table = choose("Таблица:", list(schema.tables))
    id_col = schema.column(table, "id")
    print("Какую запись изменить:")
    row_id = ask_value(id_col)
    editable = [c for c in schema.columns(table) if c.name != "id" and not c.identity]
    picked = choose("Какие поля изменить:", editable, allow_many=True)
    print("Новые значения (пусто - NULL):")
    values = [ask_value(c, allow_empty=c.nullable) for c in picked]
    sets = sql.SQL(", ").join(
        sql.SQL("{} = {}").format(sql.Identifier(c.name), ph(c)) for c in picked
    )
    q = sql.SQL("UPDATE {} SET {} WHERE id = %s").format(sql.Identifier(table), sets)
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(q, (*values, row_id))
        n = cur.rowcount
    if n:
        log.info("Запись %s в таблице %s обновлена", row_id, table)
    else:
        log.info("Запись с id=%s в таблице %s не найдена", row_id, table)


def update_many(conn, schema):
    table = choose("Таблица:", list(schema.tables))
    editable = [c for c in schema.columns(table) if c.name != "id" and not c.identity]
    target = choose("Какое поле изменить:", editable)
    print("Новое значение (пусто - NULL):")
    value = ask_value(target, allow_empty=target.nullable)
    key = choose("По какой колонке отбирать записи:", schema.columns(table))
    keys = ask_values_list(key)
    # в IN по плейсхолдеру на каждое значение списка: IN (%s, %s, ...),
    # сами значения уходят параметрами
    q = sql.SQL("UPDATE {} SET {} = {} WHERE {} IN ({})").format(
        sql.Identifier(table), sql.Identifier(target.name), ph(target),
        sql.Identifier(key.name), sql.SQL(", ").join(ph(key) for _ in keys))
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(q, (value, *keys))
        n = cur.rowcount
    log.info("Обновлено записей в таблице %s: %d", table, n)


def insert_one(conn, schema):
    table = choose("Таблица:", list(schema.tables))
    cols = insert_columns(schema, table)
    print("Значения:")
    values = [ask_value(c, allow_empty=c.nullable) for c in cols]
    q = sql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING id").format(
        sql.Identifier(table),
        sql.SQL(", ").join(sql.Identifier(c.name) for c in cols),
        sql.SQL(", ").join(ph(c) for c in cols))
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(q, values)
        new_id = cur.fetchone()[0]
    log.info("В таблицу %s добавлена запись id=%s", table, new_id)


def insert_many(conn, schema):
    table = choose("Таблица:", list(schema.tables))
    cols = insert_columns(schema, table)
    rows = []
    while True:
        print(f"Строка {len(rows) + 1}:")
        rows.append([ask_value(c, allow_empty=c.nullable) for c in cols])
        if ask("Добавить ещё строку? (да/пусто - нет): ").strip().lower() not in ("да", "д", "y"):
            break
    # колонки передаются массивами и разворачиваются unnest, получается
    # один INSERT на любое количество строк
    columns = [list(col_values) for col_values in zip(*rows)]
    q = sql.SQL("INSERT INTO {} ({}) SELECT * FROM unnest({}) RETURNING id").format(
        sql.Identifier(table),
        sql.SQL(", ").join(sql.Identifier(c.name) for c in cols),
        sql.SQL(", ").join(ph_array(c) for c in cols))
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(q, columns)
        ids = [r[0] for r in cur.fetchall()]
    log.info("В таблицу %s добавлено записей: %d, id: %s", table, len(ids),
             ", ".join(map(str, ids)))


def choose_relation(schema):
    labels = [f"{p} -> {c} (по {fk})" for p, c, fk in schema.relations]
    label = choose("Связанные таблицы:", labels)
    return schema.relations[labels.index(label)]


def insert_linked_one(conn, schema):
    parent, child, fk = choose_relation(schema)
    pcols = insert_columns(schema, parent)
    print(f"Запись в {parent}:")
    pvals = [ask_value(c, allow_empty=c.nullable) for c in pcols]
    ccols = insert_columns(schema, child, exclude=(fk,))
    print(f"Запись в {child} ({fk} подставится автоматически):")
    cvals = [ask_value(c, allow_empty=c.nullable) for c in ccols]

    # Один запрос: id родителя из RETURNING сразу идёт во вставку
    # дочерней строки, в программу он не возвращается
    q = sql.SQL("""
        WITH p AS (
            INSERT INTO {parent} ({pcols}) VALUES ({pvals}) RETURNING id
        )
        INSERT INTO {child} ({fk}, {ccols})
        SELECT p.id, {cvals} FROM p
        RETURNING {fk}, id
    """).format(
        parent=sql.Identifier(parent),
        pcols=sql.SQL(", ").join(sql.Identifier(c.name) for c in pcols),
        pvals=sql.SQL(", ").join(ph(c) for c in pcols),
        child=sql.Identifier(child),
        fk=sql.Identifier(fk),
        ccols=sql.SQL(", ").join(sql.Identifier(c.name) for c in ccols),
        cvals=sql.SQL(", ").join(ph(c) for c in ccols),
    )
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(q, (*pvals, *cvals))
        parent_id, child_id = cur.fetchone()
    log.info("Добавлено: %s id=%s, %s id=%s", parent, parent_id, child, child_id)


def insert_linked_many(conn, schema):
    parent, child, fk = choose_relation(schema)
    pcols = insert_columns(schema, parent)
    ccols = insert_columns(schema, child, exclude=(fk,))
    parents, children = [], []  # children: (номер родителя, значения)
    while True:
        n = len(parents) + 1
        print(f"Запись {n} в {parent}:")
        parents.append([ask_value(c, allow_empty=c.nullable) for c in pcols])
        while True:
            print(f"  Дочерняя запись в {child} для записи {n}:")
            children.append((n, [ask_value(c, allow_empty=c.nullable) for c in ccols]))
            if ask("  Ещё дочернюю запись? (да/пусто - нет): ").strip().lower() not in ("да", "д", "y"):
                break
        if ask(f"Ещё запись в {parent}? (да/пусто - нет): ").strip().lower() not in ("да", "д", "y"):
            break

    # Всё одним запросом. Сначала заранее берём из последовательности
    # столько id, сколько родительских строк, и связываем их с номерами строк.
    # Так дочерние строки точно получают id своего родителя.
    pcol_arrays = [list(v) for v in zip(*parents)]
    child_nums = [num for num, _ in children]
    ccol_arrays = [list(v) for v in zip(*(vals for _, vals in children))]

    pnames = [sql.Identifier(c.name) for c in pcols]
    cnames = [sql.Identifier(c.name) for c in ccols]
    q = sql.SQL("""
        WITH ids AS MATERIALIZED (
            SELECT n, nextval(pg_get_serial_sequence(%s, 'id')) AS id
            FROM generate_series(1, %s) AS n
        ),
        pin AS (
            SELECT * FROM unnest({parrays}) WITH ORDINALITY AS t({pnames}, n)
        ),
        p AS (
            INSERT INTO {parent} (id, {pnames})
            OVERRIDING SYSTEM VALUE
            SELECT ids.id, {pin_cols} FROM pin JOIN ids USING (n)
            RETURNING id
        ),
        c AS (
            INSERT INTO {child} ({fk}, {cnames})
            SELECT ids.id, {cin_cols}
            FROM unnest(%s::int4[], {carrays}) AS t(n, {cnames})
            JOIN ids USING (n)
            RETURNING id
        )
        SELECT (SELECT array_agg(id ORDER BY id) FROM p),
               (SELECT count(*) FROM c)
    """).format(
        parrays=sql.SQL(", ").join(ph_array(c) for c in pcols),
        pnames=sql.SQL(", ").join(pnames),
        parent=sql.Identifier(parent),
        pin_cols=sql.SQL(", ").join(sql.SQL("pin.{}").format(n) for n in pnames),
        child=sql.Identifier(child),
        fk=sql.Identifier(fk),
        cnames=sql.SQL(", ").join(cnames),
        carrays=sql.SQL(", ").join(ph_array(c) for c in ccols),
        cin_cols=sql.SQL(", ").join(sql.SQL("t.{}").format(n) for n in cnames),
    )
    seq_table = sql.Identifier(parent).as_string(conn)
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(q, (seq_table, len(parents), *pcol_arrays, child_nums, *ccol_arrays))
        parent_ids, child_count = cur.fetchone()
    log.info("Добавлено: в %s %d записей (id: %s), в %s %d записей",
             parent, len(parent_ids), ", ".join(map(str, parent_ids)), child, child_count)


MENU = [
    ("Просмотр таблицы без фильтра", select_all),
    ("Просмотр с фильтром по одной колонке", lambda c, s: select_filtered(c, s, many=False)),
    ("Просмотр с фильтром по нескольким колонкам", lambda c, s: select_filtered(c, s, many=True)),
    ("Изменить одну запись", update_one),
    ("Изменить несколько записей общим значением", update_many),
    ("Добавить одну строку", insert_one),
    ("Добавить одну строку в связанные таблицы", insert_linked_one),
    ("Добавить несколько строк в таблицу", insert_many),
    ("Добавить несколько строк в связанные таблицы", insert_linked_many),
]


def ask_password(prompt):
    """Скрытый ввод пароля. Без терминала (ввод из канала, docker compose run -T)
    getpass печатает предупреждение Python; там эхо и так не видно, пароль
    читается строкой из stdin."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", getpass.GetPassWarning)
        try:
            return getpass.getpass(prompt)
        except getpass.GetPassWarning:
            pass
    print(prompt, end="", flush=True)
    line = sys.stdin.readline()
    if not line:
        raise InputCancelled
    print()
    return line.removesuffix("\n")


def main():
    try:
        setup_logging()
    except ConfigError as e:
        log.error("%s", e)
        return 1
    try:
        cfg = load_config()
    except ConfigError as e:
        log.error("настройки: %s", e)
        return 1

    try:
        user = ask("Логин: ").strip()
        password = ask_password("Пароль: ")
    except (InputCancelled, KeyboardInterrupt):
        log.error("ввод отменён")
        return 1
    if not user or not password or "\x00" in user + password:
        log.error("логин и пароль не могут быть пустыми")
        return 1

    try:
        conn = connect(cfg, user, password)
        schema = Schema(conn)
    except psycopg.Error as e:
        log.error("не удалось подключиться к базе данных: проверьте логин, пароль "
                  "и доступность сервера")
        raw.error("connect: %s: %s", type(e).__name__, str(e).strip())
        return 2
    log.info("Подключение к базе %s выполнено", cfg["dbname"])
    if not schema.tables:
        log.error("нет доступных таблиц")
        return 3

    with conn:
        while True:
            print()
            try:
                title = choose("Меню:", [t for t, _ in MENU])
            except (InputCancelled, KeyboardInterrupt):
                print()
                log.info("Выход")
                return 0
            action = dict(MENU)[title]
            try:
                action(conn, schema)
            except InputCancelled:
                print("Отменено")
            except KeyboardInterrupt:
                print("\nОтменено")
            except psycopg.OperationalError as e:
                report_db_error(e, title)
                if conn.closed:
                    return 2
            except psycopg.Error as e:
                report_db_error(e, title)


if __name__ == "__main__":
    sys.exit(main())
