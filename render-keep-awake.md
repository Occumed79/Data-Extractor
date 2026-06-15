# Render Keep-Awake

No standard Node package file was found at the repository root, so no framework-specific health endpoint was added automatically.

For Render web services, add or use a lightweight endpoint that returns HTTP 200, then monitor it externally.

Recommended endpoint path:

```txt
/api/health
```

Recommended response:

```json
{ "ok": true, "awake": true }
```

Use the deployed Render URL with the health path:

```txt
https://YOUR-RENDER-SERVICE.onrender.com/api/health
```

Ping interval recommendation: every 10-14 minutes.

Common uptime monitors:

- UptimeRobot
- Better Stack
- cron-job.org

Do not ping faster than needed. This is only intended to reduce cold starts on Render web services.
