# Образ PostgreSQL 18 с русской локалью.
# В официальном образе есть только en_US, поэтому просто задать LANG в compose
# недостаточно: initdb не найдёт ru_RU и откатится на en_US.UTF-8.
FROM postgres:18.6-bookworm

RUN localedef -i ru_RU -c -f UTF-8 -A /usr/share/locale/locale.alias ru_RU.UTF-8

ENV LANG=ru_RU.utf8 \
    TZ=Europe/Moscow
