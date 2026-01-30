import requests

TG_BOT_TOKEN = ""
CHAT_ID = ""  # 그룹 chat_id

msg = "✅ Telegram notifier test message"

r = requests.post(
    f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
    json={
        "chat_id": CHAT_ID,
        "text": msg,
        "disable_web_page_preview": True
    },
    timeout=15
)

print("STATUS:", r.status_code)
print(r.text)
r.raise_for_status()
