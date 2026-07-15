"""HTTP delivery adapters for durable outbox tasks."""

from __future__ import annotations

import httpx

from tinvest_signal_engine.application.delivery import DeliveryFailure
from tinvest_signal_engine.config import RuntimeSettings, load_secret
from tinvest_signal_engine.domain.reliable_processing import DeliveryTask
from tinvest_signal_engine.models import TriggerSignal
from tinvest_signal_engine.sinks import TelegramAlertSink, WebhookAlertSink


class ConfiguredDeliverySender:
    def __init__(self, settings: RuntimeSettings) -> None:
        self._webhook = WebhookAlertSink(settings.alert_webhook_url)
        self._telegram = TelegramAlertSink(
            bot_token=settings.telegram_bot_token,
            chat_id=settings.telegram_chat_id,
            message_thread_id=settings.telegram_message_thread_id,
        )

    def send(self, task: DeliveryTask) -> None:
        signal = TriggerSignal.from_dict(task.payload)
        try:
            if task.destination_type == "webhook":
                webhook = self._current_webhook_sink()
                try:
                    if not webhook.enabled:
                        raise DeliveryFailure("webhook_not_configured")
                    webhook.send(signal)
                finally:
                    webhook.close()
                return
            if task.destination_type == "telegram":
                telegram = self._current_telegram_sink()
                try:
                    if not telegram.enabled:
                        raise DeliveryFailure("telegram_not_configured")
                    telegram.send(signal)
                finally:
                    telegram.close()
                return
            raise DeliveryFailure("unsupported_destination")
        except DeliveryFailure:
            raise
        except httpx.TimeoutException as error:
            raise DeliveryFailure("delivery_timeout") from error
        except httpx.HTTPStatusError as error:
            code = error.response.status_code
            raise DeliveryFailure(f"delivery_http_{code}") from error
        except httpx.RequestError as error:
            raise DeliveryFailure("delivery_network_error") from error
        except Exception as error:
            raise DeliveryFailure("delivery_error") from error

    def _current_webhook_sink(self) -> WebhookAlertSink:
        webhook_url = (
            load_secret("ALERT_WEBHOOK_URL", service_name="delivery_worker")
            or self._webhook._webhook_url
        )
        return WebhookAlertSink(webhook_url)

    def _current_telegram_sink(self) -> TelegramAlertSink:
        bot_token = (
            load_secret("TELEGRAM_BOT_TOKEN", service_name="delivery_worker")
            or self._telegram._bot_token
        )
        chat_id = (
            load_secret("TELEGRAM_CHAT_ID", service_name="delivery_worker")
            or self._telegram._chat_id
        )
        thread_raw = load_secret(
            "TELEGRAM_MESSAGE_THREAD_ID", service_name="delivery_worker"
        )
        message_thread_id = self._telegram._message_thread_id
        if thread_raw is not None:
            stripped_thread = thread_raw.strip()
            message_thread_id = int(stripped_thread) if stripped_thread else None
        return TelegramAlertSink(
            bot_token=bot_token,
            chat_id=chat_id,
            message_thread_id=message_thread_id,
        )

    def close(self) -> None:
        self._webhook.close()
        self._telegram.close()
