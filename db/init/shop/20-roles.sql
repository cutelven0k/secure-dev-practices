-- Права пользователя приложения. Таблицы принадлежат администратору,
-- приложение может только читать, добавлять и изменять строки.
-- DELETE, TRUNCATE, создание и изменение таблиц ему недоступны.

GRANT SELECT, INSERT, UPDATE ON authors, books, customers, orders, order_items TO :"db_user";

-- USAGE на последовательности нужен для nextval() при вставке
-- нескольких заказов с позициями одним запросом
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO :"db_user";
