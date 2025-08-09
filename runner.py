import os, json
from datetime import datetime, timezone, timedelta
import requests
from pymongo import MongoClient, ReturnDocument

MONGO_URI = os.environ["MONGO_URI"]
DB = os.getenv("MONGO_DB", "telegram")
COLL = os.getenv("MONGO_COLL", "tasks")
BOT = os.environ["TG_BOT_TOKEN"]
PRUNE_DAYS_SENT = int(os.getenv("PRUNE_DAYS_SENT", "30"))
PRUNE_DAYS_ERROR = int(os.getenv("PRUNE_DAYS_ERROR", "60"))

col = MongoClient(MONGO_URI)[DB][COLL]

def prune_old(now):
    try:
        if PRUNE_DAYS_SENT > 0:
            cutoff = now - timedelta(days=PRUNE_DAYS_SENT)
            col.delete_many({"status":"sent", "sent_at":{"$lt": cutoff}})
        if PRUNE_DAYS_ERROR > 0:
            cutoff_e = now - timedelta(days=PRUNE_DAYS_ERROR)
            col.delete_many({"status":"error", "started_at":{"$lt": cutoff_e}})
    except Exception as e:
        print("Prune error:", e)

def claim_task(now):
    return col.find_one_and_update(
        {"status":"pending", "run_at":{"$lte": now}},
        {"$set":{"status":"sending", "started_at": now}},
        sort=[("run_at", 1)],
        return_document=ReturnDocument.AFTER
    )

def send_audio(task):
    url = f"https://api.telegram.org/bot{BOT}/sendAudio"
    data = {
        "chat_id": task["channel_id"],            # может быть numeric или @username
        "audio": task["file_id"],                 # file_id => без multipart
        "caption": task.get("caption_html",""),
        "parse_mode": "HTML",
        "reply_markup": json.dumps(task.get("keyboard", {})),
        "disable_notification": False,
    }
    r = requests.post(url, data=data, timeout=30)
    j = r.json()
    if not j.get("ok"):
        raise RuntimeError(j)
    return j["result"]["message_id"]

def main():
    now = datetime.now(timezone.utc)
    prune_old(now)

    processed = 0
    while True:
        t = claim_task(now)
        if not t:
            break
        try:
            mid = send_audio(t)
            col.update_one(
                {"_id": t["_id"]},
                {"$set":{"status":"sent","sent_at": datetime.now(timezone.utc),"message_id": mid}}
            )
            processed += 1
        except Exception as e:
            col.update_one(
                {"_id": t["_id"]},
                {"$set":{"status":"error","error": str(e)}, "$inc":{"attempts":1}}
            )
    print(f"Processed: {processed}")

if __name__ == "__main__":
    main()
