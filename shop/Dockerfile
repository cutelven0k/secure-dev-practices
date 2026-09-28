FROM python:3.13.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=Europe/Moscow

RUN useradd --system --uid 10002 --no-create-home --shell /usr/sbin/nologin shop \
    && mkdir /logs && chown shop /logs

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shop.py config.json ./

USER shop
ENTRYPOINT ["python", "shop.py"]
