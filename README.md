# Provider Data Portal — Render-ready package

This package is set up for **GitHub + Render Blueprint deployment** with:

- Flask web app
- Redis + RQ background worker
- Render Key Value for queueing
- Render Postgres for shared persistence
- Playwright running inside a Docker image that already includes Chromium and browser dependencies
- CSV / JSON / XLSX / PDF exports
- MDsave + Zocdoc extraction in one portal

## Why this version changed

The previous SQLite-based build was fine locally, but it was **not truly Render-ready** for a web service + worker split because the web and worker would not share the same local filesystem. This package switches job and result storage to **Postgres**, which both services can use.

I also switched the Render deployment to **Docker runtime** because Playwright is much more reliable there than in a plain native Python runtime.

## Deploy on Render

1. Create a new GitHub repo.
2. Upload the contents of this folder to the repo root.
3. In Render, choose **New + > Blueprint**.
4. Connect the GitHub repo.
5. Render will read `render.yaml` and create:
   - one web service
   - one background worker
   - one Key Value instance
   - one Postgres database
6. Approve the Blueprint.

## Local run

You can still run this locally.

### 1) Start Redis and Postgres locally

Example with Docker:

```bash
docker run --name portal-redis -p 6379:6379 redis:7
docker run --name portal-postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres -e POSTGRES_DB=provider_portal -p 5432:5432 postgres:16
```

### 2) Set environment variables

```bash
export REDIS_URL=redis://localhost:6379/0
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/provider_portal
```

### 3) Install and run

```bash
pip install -r requirements.txt
python app.py
python worker.py
```

Then open `http://localhost:5000`

## Render notes

- Render recommends **Gunicorn** for Flask web services. Their Flask quickstart uses `gunicorn app:app`. citeturn342116view2
- Render background workers are meant for queue-driven async work, typically backed by **Render Key Value**. citeturn342116view1turn342116view3
- In Blueprints, a Key Value instance is declared with `type: keyvalue`, and `redis` is now just a deprecated alias. citeturn735426view0
- Render Blueprints are defined in a root `render.yaml` file and can provision interconnected services together. citeturn342116view0turn988233search10

## Important honesty notes

- This is **deployable**, but it is still a starter app, not a hardened commercial scraping platform.
- Zocdoc and MDsave can change markup or deploy anti-bot measures at any time.
- Playwright crawling can consume significant memory on small plans.
- If you want login, multi-user accounts, audit trails, retries, or stronger observability, those should be the next upgrades.
