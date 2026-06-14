# Security Policy

## Secrets

Never commit T-Invest tokens, Telegram bot tokens, chat IDs, Postgres passwords, or admin tokens.

Keep runtime secrets in `.env`, Docker secrets, CI secrets, or a private deployment environment. Public docs and screenshots must not include real tokens, account IDs, chat IDs, portfolio IDs, or broker API payloads that identify a user.

## Reporting

Open a private security report through GitHub if the repository supports it. If not, contact the repository owner directly and do not publish exploit details in an issue.

## Operational Notes

- Rotate `TINVEST_TOKEN`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `ADMIN_API_TOKEN`, and database credentials after accidental exposure.
- Treat `/admin` and `/admin/api/*` as private surfaces. They require `X-Admin-Token` and should stay behind trusted network access in production.
- Runtime fingerprint fields are safe to expose in health/admin responses; they intentionally include version, commit SHA, and build time, not secrets.
