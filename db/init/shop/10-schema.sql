-- Книжный магазин: авторы, книги, покупатели, заказы и позиции заказов.

CREATE TABLE authors (
    id        integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    full_name text    NOT NULL CHECK (length(full_name) BETWEEN 1 AND 200),
    country   text
);

CREATE TABLE books (
    id        integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    author_id integer       NOT NULL REFERENCES authors (id),
    title     text          NOT NULL CHECK (length(title) BETWEEN 1 AND 300),
    year      integer       CHECK (year BETWEEN 1400 AND 2100),
    price     numeric(10,2) NOT NULL CHECK (price >= 0),
    stock     integer       NOT NULL DEFAULT 0 CHECK (stock >= 0)
);

CREATE TABLE customers (
    id        integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    full_name text NOT NULL CHECK (length(full_name) BETWEEN 1 AND 200),
    email     text NOT NULL UNIQUE CHECK (email LIKE '%_@_%'),
    city      text
);

CREATE TABLE orders (
    id          integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id integer NOT NULL REFERENCES customers (id),
    created_at  date    NOT NULL DEFAULT current_date,
    status      text    NOT NULL DEFAULT 'new'
                CHECK (status IN ('new', 'paid', 'shipped', 'done', 'cancelled'))
);

CREATE TABLE order_items (
    id       integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id integer       NOT NULL REFERENCES orders (id),
    book_id  integer       NOT NULL REFERENCES books (id),
    count    integer       NOT NULL CHECK (count > 0),
    price    numeric(10,2) NOT NULL CHECK (price >= 0)
);

CREATE INDEX ON books (author_id);
CREATE INDEX ON orders (customer_id);
CREATE INDEX ON order_items (order_id);
CREATE INDEX ON order_items (book_id);
