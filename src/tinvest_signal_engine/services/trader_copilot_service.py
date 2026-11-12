"""Trade Desk Copilot service: interactive CLI and Telegram background listener."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import logging
import os
import signal
import sys
import time
from typing import Any

import httpx

from tinvest_signal_engine.adapters.gemini_trader import GeminiTraderCopilotAdapter
from tinvest_signal_engine.adapters.internet_news_search import GoogleNewsRssSearchAdapter
from tinvest_signal_engine.adapters.market_data_provider import CompositeMarketDataProvider
from tinvest_signal_engine.application.trader_copilot import TraderCopilotUseCase
from tinvest_signal_engine.config import load_secret
from tinvest_signal_engine.domain.trader_copilot import (
    TraderDeskQuery,
    format_trader_response_html,
)

logger = logging.getLogger(__name__)


def build_copilot_use_case(*, api_key: str | None = None) -> TraderCopilotUseCase:
    effective_key = api_key or os.getenv("GEMINI_API_KEY") or load_secret("GEMINI_API_KEY")
    if not effective_key:
        raise ValueError("GEMINI_API_KEY is required for Trade Desk Copilot")

    llm_adapter = GeminiTraderCopilotAdapter(api_key=effective_key)
    market_provider = CompositeMarketDataProvider()
    news_search = GoogleNewsRssSearchAdapter()

    return TraderCopilotUseCase(
        llm=llm_adapter,
        market_data=market_provider,
        news_search=news_search,
    )


def run_cli_query(query_text: str, *, ticker: str | None = None) -> None:
    use_case = build_copilot_use_case()
    query = TraderDeskQuery(query_text=query_text, ticker=ticker)
    response, market = use_case.answer_query(query)
    html_output = format_trader_response_html(response, market_context=market, query_text=query_text)
    print("\n=== КОНСУЛЬТАЦИЯ ТРЕЙДИНГ-ДЕСКА ===")
    print(html_output)
    print("===================================\n")


def run_telegram_listener(
    *,
    bot_token: str | None = None,
    poll_timeout_seconds: int = 25,
) -> None:
    token = (
        bot_token
        or os.getenv("TELEGRAM_BOT_TOKEN")
        or load_secret("TELEGRAM_BOT_TOKEN", service_name="delivery-worker")
    )
    if not token:
        token_file = os.getenv("TELEGRAM_BOT_TOKEN_FILE")
        if token_file and os.path.isfile(token_file):
            try:
                token = open(token_file).read().strip()
            except Exception:
                pass

    if not token:
        logger.error("TELEGRAM_BOT_TOKEN is not configured")
        sys.exit(1)

    use_case = build_copilot_use_case()
    logger.info("Trade Desk Copilot Telegram listener starting...")

    # Identify bot username
    bot_username = "ost_ap_bot"
    with httpx.Client(timeout=10.0, trust_env=False) as http_client:
        try:
            me_resp = http_client.get(f"https://api.telegram.org/bot{token}/getMe")
            if me_resp.status_code == 200:
                bot_username = str(me_resp.json().get("result", {}).get("username") or "ost_ap_bot").lower()
                logger.info("Connected as bot @%s", bot_username)
        except Exception as ex:
            logger.warning("Could not fetch getMe: %s", ex)

    stop_requested = False

    def _sig_handler(signum: int, frame: Any) -> None:
        nonlocal stop_requested
        logger.info("Shutdown requested")
        stop_requested = True

    import threading
    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGINT, _sig_handler)
        signal.signal(signal.SIGTERM, _sig_handler)

    offset = 0
    client = httpx.Client(timeout=poll_timeout_seconds + 5.0, trust_env=False)

    try:
        while not stop_requested:
            try:
                url = f"https://api.telegram.org/bot{token}/getUpdates?offset={offset}&timeout={poll_timeout_seconds}"
                resp = client.get(url)
                if resp.status_code != 200:
                    logger.warning("getUpdates returned status %s: %s", resp.status_code, resp.text)
                    time.sleep(3.0)
                    continue

                data = resp.json()
                updates = data.get("result", [])
                for upd in updates:
                    upd_id = upd.get("update_id", 0)
                    offset = max(offset, upd_id + 1)

                    message = upd.get("message")
                    if not message or "text" not in message:
                        continue

                    text = str(message.get("text") or "").strip()
                    chat = message.get("chat", {})
                    chat_id = chat.get("id")
                    chat_type = chat.get("type", "private")
                    msg_id = message.get("message_id")
                    thread_id = message.get("message_thread_id")
                    reply_to = message.get("reply_to_message")
                    user = message.get("from", {})
                    user_name = user.get("username") or user.get("first_name") or "Трейдер"

                    # Determine if message is addressed to the bot
                    is_direct_chat = chat_type == "private"
                    is_command = any(text.startswith(cmd) for cmd in ("/ask", "/trader", "/copilot", "/ai", "/desk", "/анализ"))
                    is_mention = f"@{bot_username}" in text.lower()
                    is_reply_to_bot = reply_to and reply_to.get("from", {}).get("is_bot")

                    if not (is_direct_chat or is_command or is_mention or is_reply_to_bot):
                        continue

                    # Clean prefix/mention
                    cleaned_text = text
                    if is_mention:
                        cleaned_text = cleaned_text.replace(f"@{bot_username}", "").strip()
                    for cmd in ("/ask", "/trader", "/copilot", "/ai", "/desk", "/анализ"):
                        if cleaned_text.startswith(cmd):
                            cleaned_text = cleaned_text[len(cmd):].strip()
                            break

                    reply_text = str(reply_to.get("text") or "") if reply_to else None

                    logger.info("Processing copilot query from %s: '%s'", user_name, cleaned_text)

                    desk_query = TraderDeskQuery(
                        query_text=cleaned_text,
                        user_name=user_name,
                        chat_id=chat_id,
                        message_id=msg_id,
                        reply_to_text=reply_text,
                    )

                    copilot_resp, market_ctx = use_case.answer_query(desk_query)
                    out_html = format_trader_response_html(
                        copilot_resp, market_context=market_ctx, query_text=cleaned_text
                    )

                    # Send reply back
                    send_payload: dict[str, Any] = {
                        "chat_id": chat_id,
                        "text": out_html,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": True,
                        "reply_to_message_id": msg_id,
                    }
                    if thread_id is not None:
                        send_payload["message_thread_id"] = thread_id

                    send_resp = client.post(
                        f"https://api.telegram.org/bot{token}/sendMessage",
                        json=send_payload,
                    )
                    if send_resp.status_code != 200:
                        logger.error("Failed to send copilot reply: %s %s", send_resp.status_code, send_resp.text)
                    else:
                        logger.info("Sent copilot consultation to chat %s (message %s)", chat_id, msg_id)

            except Exception as ex:
                if not stop_requested:
                    logger.error("Error in Telegram listener loop: %s", ex)
                    time.sleep(2.0)
    finally:
        client.close()
        logger.info("Telegram listener stopped")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    parser = argparse.ArgumentParser(description="Trade Desk Copilot")
    parser.add_argument("--ask", type=str, help="Question to the Trade Desk")
    parser.add_argument("--ticker", type=str, help="Specific instrument ticker")
    parser.add_argument("--listen", action="store_true", help="Start background Telegram listener")
    args = parser.parse_args()

    if args.listen:
        run_telegram_listener()
    elif args.ask:
        run_cli_query(args.ask, ticker=args.ticker)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
