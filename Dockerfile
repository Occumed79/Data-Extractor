FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .

ENV PORT=10000
EXPOSE 10000

CMD ["/bin/sh", "-c", "gunicorn --bind 0.0.0.0:${PORT:-10000} app:app"]
