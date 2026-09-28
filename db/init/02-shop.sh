#!/bin/bash
# Создание таблиц книжного магазина, прав и демо-данных в базе приложения.
# Официальный образ выполняет только файлы из корня initdb.d,
# поэтому SQL лежит в подкаталоге shop/ и запускается отсюда по порядку.
set -euo pipefail

for f in /docker-entrypoint-initdb.d/shop/*.sql; do
    echo "shop init: $f"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$APP_DB_NAME" \
         -v db_user="$APP_DB_USER" -f "$f"
done
