FROM python:3.13.12-slim-bookworm

# PYTHONUNBUFFERED: stdout/stderr без буфера, иначе логи в docker compose logs
# появляются пачками с задержкой, а не в момент проверки
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=Europe/Moscow

# отдельный пользователь без прав root
RUN useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin pinger \
    && mkdir /logs && chown pinger /logs

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pinger.py config.json ./

USER pinger
STOPSIGNAL SIGTERM
ENTRYPOINT ["python", "pinger.py"]
