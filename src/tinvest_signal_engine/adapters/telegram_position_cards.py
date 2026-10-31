"""Send one generated asset image and its accessible caption in one API call."""

from tinvest_signal_engine.adapters.asset_position_cards import render_asset_positions_html, render_asset_positions_png


def send_position_card(*, client, bot_token: str, chat_id: str,
                       message_thread_id: int | None, payload: dict) -> None:
    caption = render_asset_positions_html(payload)
    photo = render_asset_positions_png(payload)
    data = {"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"}
    if message_thread_id is not None:
        data["message_thread_id"] = str(message_thread_id)
    response = client.post(f"https://api.telegram.org/bot{bot_token}/sendPhoto", data=data,
                           files={"photo": (payload["commodity"] + ".png", photo, "image/png")})
    response.raise_for_status()
    if not response.json().get("ok"):
        raise RuntimeError("Telegram rejected position card")
