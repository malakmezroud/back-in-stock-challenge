# Restock

A dependency-free back-in-stock alert service built with Python, SQLite, and a durable delivery worker. The customer form, password-protected admin page, and responsive HTML email are included.

## Run locally

Requires Python 3.12 or later. There are no pip dependencies.

```sh
cp .env.example .env
# Fill .env with the challenge test-server values; do not commit it.
set -a
. ./.env
set +a
python app.py
```

Open `/embed` for the iframe form and `/admin` for operations. Basic auth username: `admin`. Set `ADMIN_PASSWORD` for a predictable password. If absent, the application generates a random password on first startup, saves it to the database, and prints it once in the startup log. Use HTTPS for hosted admin access.

## Docker

```sh
docker build -t restock .
docker run --rm -p 8080:8080 --env-file .env -v restock-data:/data restock
```

The image listens on `$PORT` (default 8080). The Dockerfile declares an anonymous `/data` volume, so the container also starts with only the required environment variables. Use a **named** volume, as above, to retain the database when replacing a container. With `--rm`, anonymous volumes are deleted; named volumes are not. Set `DB_PATH` if using another durable disk location.

Docker is unavailable in the build environment used for this draft, so an actual image build/run has not yet been verified. The included GitHub Actions workflow builds it and checks `/health` when the repository is published.

## Environment

| Variable | Purpose |
| --- | --- |
| `STOCK_API_URL` | Stock catalogue base; app requests `{base}/{urlencoded SKU}` |
| `STOCK_API_KEY` | Bearer credential for stock API |
| `RESEND_BASE_URL` | Mock/test Resend base; app posts to `/emails`, or uses it as-is when it already ends in `/emails` |
| `RESEND_API_KEY` | Bearer credential for email API |
| `WEBHOOK_SECRET` | Required `X-Webhook-Secret` value; webhooks fail closed when unset |
| `ADMIN_PASSWORD` | Optional operations password; otherwise generated and persisted |
| `FROM_EMAIL` | Sender, default `Restock <alerts@example.com>`; set a verified sender for a real provider |
| `PORT` | Listener port, default 8080 |
| `DB_PATH` | Durable SQLite path, default `data/restock.sqlite3` locally and `/data/restock.sqlite3` in Docker |
| `POLL_SECONDS` | Availability reconciliation interval per waiting request, default 10 |

No real production credentials or customer data are included. Point both APIs at the challenge harness for evaluation. Outbound email delivery is disabled by missing configuration, not silently redirected to a live service.

## HTTP contract

- `GET /embed?sku=TEE-ORG-WHT-M` — embeddable signup form, with optional item prefill.
- `POST /api/requests` — JSON `{ "email": "alex@example.com", "sku": "TEE-ORG-WHT-M" }`; returns 201 on creation, 200 for an already-open normalized email/SKU pair.
- `POST /webhooks/stock` — JSON `{ "event_id": "event-123", "sku": "TEE-ORG-WHT-M", "quantity": 4 }`, with `X-Webhook-Secret`.
- `GET /admin` — password-protected request history, delivery state, attempts, errors, and notification timestamps. Shows the most recent 500 records.
- `GET /health` — listener/storage health check, not proof that external APIs are reachable.

The form is designed for an iframe. The API also accepts server-to-server JSON callers such as the supplied harness. Cross-origin browser CORS access is not enabled. The read-only admin page cannot be framed.

## Avoiding duplicate and missed emails

1. A partial SQLite unique index permits only one open request per normalized email/SKU. It remains open while waiting, sending, or retrying. A new request can be created after the prior one is notified.
2. SQLite `BEGIN IMMEDIATE` and a unique event key deduplicate concurrent repeated webhooks. Reuse of an event ID with a different SKU/quantity is rejected.
3. The stock API is authoritative. A webhook claiming stock is available never suffices by itself. Each send rechecks availability. Periodic reconciliation also covers missed webhooks, temporary stock API failures, and already-in-stock signups.
4. The subscription row is a durable outbox. A worker acquires a short database lease. The first-send timestamp is committed before contacting the email provider. Pending requests and expired leases survive process restarts.
5. Every retry uses `Idempotency-Key: restock/{request_id}` with the same payload. Provider acknowledgement is persisted with the notification timestamp. If a response is lost after provider acceptance, a retry with the same key avoids sending twice, provided the provider honors that key.
6. Failures back off, capped at 60 seconds. Other subscriptions continue to be processed. No secrets or raw error responses are stored in the admin error column.
7. After 23 hours from the first attempted send, unresolved deliveries move to `needs_review` rather than risk repeating an accepted email beyond the provider's idempotency retention window. This is visible in the admin page. A hard guarantee of exactly-once delivery is impossible without provider support and bounded recovery. Do not change sender/payload configuration during an uncertain delivery.

## Validation completed

```sh
python -m unittest -v test_app
```

14 tests pass: concurrent signups, normalized duplicates, validation, webhook authentication, conflicting event IDs, authoritative stock checks, repeated concurrent webhooks, failed email retries, uncertain provider outcomes, persisted recovery, worker leases, repeat subscription lifecycle, password-protected admin/timestamps, endpoint availability, and old-uncertain-delivery review handling.

A separate smoke test against a locally running copy of the official public harness also passed: duplicate signups, simultaneous webhook delivery, an injected email failure, retry with a stable key, one accepted HTML email, and a real app-process restart without another accepted email. The private grader is not public; no official hosted score is claimed.

## Boundaries

This is a challenge-sized service, not a fully hardened bulk-email SaaS. A production rollout should add per-tenant isolation, abuse controls/email ownership verification, retention/deletion tooling, worker monitoring, and provider reconciliation for `needs_review` rows. SQLite on a shared persistent disk supports this single-host design; use a managed database/queue for multi-host scaling.

## AI assistance and timing

Built using OpenAI Codex in ChatGPT. Claude Code was not used. Exact total completion time was not tracked; do not represent this work as personally completed without AI assistance. The applicant should review and understand the code before submitting it and confirm the employer's 7-hours-per-day working arrangement.
