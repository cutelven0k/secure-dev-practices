#!/bin/bash
# Выполняется один раз при первой инициализации кластера (пустой volume).
# Создаёт базу с русской локалью и пользователя приложения с минимальными правами.
# Имена и пароль передаются в psql как переменные (:"name" / :'value'),
# psql сам их экранирует, поэтому строка SQL не склеивается из env.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
     -v db_name="$APP_DB_NAME" \
     -v db_user="$APP_DB_USER" \
     -v db_pass="$APP_DB_PASSWORD" <<'EOSQL'
CREATE ROLE :"db_user" LOGIN PASSWORD :'db_pass';

CREATE DATABASE :"db_name"
    TEMPLATE template0
    ENCODING 'UTF8'
    LOCALE 'ru_RU.utf8';

-- по умолчанию CONNECT к новой базе есть у всех (PUBLIC), убираем
REVOKE ALL ON DATABASE :"db_name" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"db_name" TO :"db_user";

\connect :"db_name"
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO :"db_user";
EOSQL
