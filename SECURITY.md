# Политика безопасности

## Секреты

Не коммитьте T-Invest tokens, Telegram bot tokens, chat IDs, Postgres passwords или admin tokens.

Храните runtime-секреты в `.env`, Docker secrets, CI secrets или приватном deployment environment. Публичные docs и screenshots не должны содержать реальные tokens, account IDs, chat IDs, deployment IDs или broker API payloads, по которым можно идентифицировать пользователя.

## Как сообщать о проблемах

Откройте private security report через GitHub, если repository это поддерживает. Если нет, свяжитесь с owner напрямую и не публикуйте exploit details в issue.

## Эксплуатационные заметки

- Ротируйте `TINVEST_TOKEN`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `ADMIN_API_TOKEN` и database credentials после случайной публикации.
- Считайте `/admin` и `/admin/api/*` приватными surfaces. Они требуют `X-Admin-Token` и в production должны оставаться за trusted network access.
- Runtime fingerprint fields безопасно отдавать в health/admin responses: они намеренно включают version, commit SHA и build time, но не secrets.
