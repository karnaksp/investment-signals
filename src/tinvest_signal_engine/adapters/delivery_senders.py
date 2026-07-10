"""HTTP delivery adapters for durable outbox tasks."""

from __future__ import annotations

import httpx

from tinvest_signal_engine.application.delivery import DeliveryFailure
from tinvest_signal_engine.config import RuntimeSettings
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
                if not self._webhook.enabled:
                    raise DeliveryFailure("webhook_not_configured")
                self._webhook.send(signal)
                return
            if task.destination_type == "telegram":
                if not self._telegram.enabled:
                    raise DeliveryFailure("telegram_not_configured")
                self._telegram.send(signal)
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

    def close(self) -> None:
        self._webhook.close()
        self._telegram.close()
