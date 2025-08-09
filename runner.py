import os
import json
from datetime import datetime, timezone, timedelta
import requests
from pymongo import MongoClient, ReturnDocument

MONGO_URI = os.environ["MONGO_URI"]
DB = os.getenv("MONGO_DB", "telegram")
COLL = os.getenv("MONGO_COLL", "tasks")
BOT = os.environ["TG_BOT_TOKEN"]

col = MongoClient(MONGO_URI)[DB][COLL]

def claim_task(now):
    return col.find_one_and_update(
        {"status": "pending", "run_at": {"$lte": now}},
        {"$set": {"status": "sending", "started_at": now}},
        sort=[("run_at", 1)],
        return_document=ReturnDocument.AFTER
    )

def send_audio(task):
    url = f"https://api.telegram.org/bot{BOT}/sendAudio"
    # Кнопки: Паблик | Сайт | Beatchain
    keyboard = {"inline_keyboard": [[
        {"text": "Паблик", "url": "https://vk.com/ic_beatz"},
        {"text": "Сайт", "url": task["links"][0]},
        {"text": "Beatchain", "url": task["links"][1]},
    ]]}
    data = {
        "chat_id": task["channel_id"],
        "audio": task["file_id"],  # file_id
        "reply_markup": json.dumps(keyboard),
        "parse_mode": "HTML"
    }
    r = requests.post(url, data=data, timeout=30)
    j = r.json()
    if not j.get("ok"):
        raise RuntimeError(j)
    return j["result"]["message_id"]

def cleanup_old_sent(days=7):
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    result = col.delete_many({"status": "sent", "sent_at": {"$lt": cutoff}})
    print(f"Cleaned up {result.deleted_count} old sent tasks")

def main():
    now = datetime.now(timezone.utc)
    processed = 0
    while True:
        t = claim_task(now)
        if not t:
            break
        try:
            mid = send_audio(t)
            col.update_one(
                {"_id": t["_id"]},
                {"$set": {
                    "status": "sent",
                    "sent_at": datetime.now(timezone.utc),
                    "message_id": mid
                }}
            )
            processed += 1
        except Exception as e:
            col.update_one(
                {"_id": t["_id"]},
                {"$set": {"status": "error", "error": str(e)}, "$inc": {"attempts": 1}}
            )
    cleanup_old_sent()
    print(f"Processed: {processed}")

if __name__ == "__main__":
    main()
