# File: notifications/telegram.py

import requests

# ────────────────────────────────────────────────────────────────────────────────
# Telegram Notification Utility Module
#
# Sends messages via Telegram Bot API. Bot token and chat ID are hardcoded here
# (per user request), so you don’t need environment variables or Prefect Secrets.
#
# Reference: https://core.telegram.org/bots/api#sendmessage
# ────────────────────────────────────────────────────────────────────────────────

# ─── Replace the values below with your actual bot token and chat ID ───
BOT_TOKEN = "7904221180:AAFjagpLXTo8vy5UfmA2MG8s-4Vn3Fc95kg"
CHAT_ID   = "778862180"

TELEGRAM_API_BASE = "https://api.telegram.org"

def send_telegram_message(text: str) -> None:
    """
    Send a text message to a Telegram chat via the hardcoded bot/token and chat ID.
    Raises RuntimeError if the HTTP call fails.
    """
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("Telegram BOT_TOKEN or CHAT_ID is not set in telegram.py")

    url = f"{TELEGRAM_API_BASE}/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text}

    response = requests.get(url, params=payload, timeout=10)
    if not response.ok:
        raise RuntimeError(
            f"Failed to send Telegram message: {response.status_code} {response.text}"
        )
