"""Send one generated asset image, accessible caption, and optional LLM macro analysis reply."""

import logging
from tinvest_signal_engine.adapters.asset_position_cards import (
    ASSET_NAMES,
    render_asset_positions_html,
    render_asset_positions_png,
)

logger = logging.getLogger(__name__)


def send_position_card(
    *,
    client,
    bot_token: str,
    chat_id: str,
    message_thread_id: int | None,
    payload: dict,
) -> None:
    caption = render_asset_positions_html(payload)
    photo = render_asset_positions_png(payload)
    data = {"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"}
    if message_thread_id is not None:
        data["message_thread_id"] = str(message_thread_id)
    response = client.post(
        f"https://api.telegram.org/bot{bot_token}/sendPhoto",
        data=data,
        files={"photo": (payload["commodity"] + ".png", photo, "image/png")},
    )
    response.raise_for_status()
    body = response.json()
    if not body.get("ok"):
        raise RuntimeError("Telegram rejected position card")

    # Send LLM macro analysis and long/short ratio interpretation as a reply
    _send_llm_analysis_reply(
        client=client,
        bot_token=bot_token,
        chat_id=chat_id,
        message_thread_id=message_thread_id,
        photo_message_id=body.get("result", {}).get("message_id"),
        payload=payload,
    )


def _send_llm_analysis_reply(
    *,
    client,
    bot_token: str,
    chat_id: str,
    message_thread_id: int | None,
    photo_message_id: int | None,
    payload: dict,
) -> None:
    try:
        from tinvest_signal_engine.adapters.signal_llm_factory import (
            get_global_llm_service,
        )
        from tinvest_signal_engine.domain.signal_llm import (
            format_position_analysis_html,
        )

        llm_service = get_global_llm_service()
        if llm_service is None:
            return

        analysis = llm_service.analyze_asset_positions(payload)
        if analysis is None:
            return

        commodity_title = ASSET_NAMES.get(payload.get("commodity", ""), "")
        reply_text = format_position_analysis_html(
            analysis, commodity_title=commodity_title
        )
        reply_data = {
            "chat_id": chat_id,
            "text": reply_text,
            "parse_mode": "HTML",
        }
        if photo_message_id is not None:
            reply_data["reply_to_message_id"] = str(photo_message_id)
        if message_thread_id is not None:
            reply_data["message_thread_id"] = str(message_thread_id)

        resp = client.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            data=reply_data,
        )
        if resp.status_code >= 400:
            logger.warning(
                "Telegram position analysis reply failed (HTTP %s): %s",
                resp.status_code,
                resp.text[:150],
            )
    except Exception as ex:
        logger.warning("Error generating LLM position analysis reply: %s", ex)
