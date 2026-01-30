import os, time, traceback
from dotenv import load_dotenv
from sqlalchemy import text, bindparam
from db_info import make_engine
from notifier_worker import send_notification

load_dotenv()

engine = make_engine()

TG_GROUP_CHAT_ID = int(os.environ["TG_GROUP_CHAT_ID"])
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC","60"))
FETCH_LIMIT = int(os.getenv("FETCH_LIMIT","200"))
CLAIM_BATCH = int(os.getenv("CLAIM_BATCH","50"))

def reserve_jobs(conn) -> int:

    res = conn.execute(text("""
        INSERT IGNORE INTO notifications_victims (victim_id, channel_type, channel_id)
        SELECT v.id, 'telegram_group', :chat_id
        FROM victims v
        WHERE v.created_at >= CURDATE()
            AND v.created_at < DATE_ADD(CURDATE(), INTERVAL 1 DAY)
        ORDER BY v.id ASC
        LIMIT :lim

    """), {"chat_id": TG_GROUP_CHAT_ID, "lim": FETCH_LIMIT})
    return int(getattr(res, "rowcount", 0) or 0)

def claim_pending(conn) -> list[int]:
    # 1) pending job id 가져오기
    ids = [r[0] for r in conn.execute(text("""
        SELECT id
        FROM notifications_victims
        WHERE status='pending'
          AND channel_type='telegram_group'
          AND channel_id=:chat_id
        ORDER BY id ASC
        LIMIT :lim
    """), {"chat_id": TG_GROUP_CHAT_ID, "lim": CLAIM_BATCH}).all()]

    if not ids:
        return []

    # 2) 해당 id들만 queued로 claim (경쟁 상황 대비해서 status='pending' 조건 유지)
    upd = text("""
        UPDATE notifications_victims
        SET status='queued'
        WHERE status='pending'
          AND id IN :ids
    """).bindparams(bindparam("ids", expanding=True))

    conn.execute(upd, {"ids": ids})

    # 3) 실제로 queued 된 것만 반환
    sel = text("""
        SELECT id
        FROM notifications_victims
        WHERE status='queued'
          AND id IN :ids
    """).bindparams(bindparam("ids", expanding=True))

    claimed = [r[0] for r in conn.execute(sel, {"ids": ids}).all()]
    return claimed

def run_once():
    with engine.begin() as conn:
        inserted = reserve_jobs(conn)
        if inserted:
            print(f"[notifier] reserved {inserted} new jobs")
            
        claimed = claim_pending(conn)
        
    if not claimed:
        print("[notifier] nothing to enqueue")
        return
    
    for job_id in claimed:
        send_notification.delay(job_id)
        print(f"[notifier] enqueud job_id={job_id}")
        
def main():
    print("[notifier] starting poll loop...")
    while True:
        try:
            run_once()
        except Exception as e:
            print(f"[notifier] error: {type(e).__name__}: {e}")
            traceback.print_exc()
        time.sleep(POLL_INTERVAL_SEC)
        
if __name__ == "__main__":
    main()