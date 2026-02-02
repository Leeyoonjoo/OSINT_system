import os, time, traceback
from dotenv import load_dotenv
from sqlalchemy import text, bindparam
from db_info import make_engine
from notifier_worker import send_notification

'''
코드 로직:
    DB 폴링 -> 알림 작업 예약 -> 작업 클레임(아직 안 보낸 pending을 선점) -> celery worker로 전달
'''

# 전역 설정 & 초기화 (프로그램 시작 시)
'''
.env에서 텔레그램 채팅 ID, 폴링 주기, 배치 크기를 읽음
DB 연결용 SQLAlchemy engine 생성
'''
load_dotenv()

engine = make_engine()

TG_GROUP_CHAT_ID = int(os.environ["TG_GROUP_CHAT_ID"])
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC","60"))   # 몇 초마다 DB를 확인할지
FETCH_LIMIT = int(os.getenv("FETCH_LIMIT","200"))   # 한 번에 "예약"할 최대 victim tn
CLAIM_BATCH = int(os.getenv("CLAIM_BATCH","50"))  # 한 번에 worker로 넘길 최대 job 수

# 3
# 알림 작업 "생성" - 아직 알림 작업이 없는 victime을 job으로 만들어주는 역할
def reserve_jobs(conn) -> int:
    # 오늘 생성된 victim 중 아직 notification_victims에 없는 것만 텔레그램 알림 대상으로 작업 생성 
    #  UNIQUE Key - 중복 알림 방지 
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

# 4
def claim_pending(conn) -> list[int]:
    # 1) pending job id 가져오기, 아직 아무 worker도 가져가지 않은 작업
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

    # status 변경으로 "내가 가져감" 표시 - 여러 worker가 동시에 실행돼도 이미 누가 가져간 job은 다시 못 가져가게 함.
    upd = text("""
        UPDATE notifications_victims
        SET status='queued'
        WHERE status='pending'
          AND id IN :ids
    """).bindparams(bindparam("ids", expanding=True))

    conn.execute(upd, {"ids": ids})

    # 3) 실제로 queued 된 것만 반환
    # 왜냐하면 update 경쟁 상황에서 일부 id는 다른 프로세스가 먼저 가져갔을수도 있음.
    sel = text("""
        SELECT id
        FROM notifications_victims
        WHERE status='queued'
          AND id IN :ids
    """).bindparams(bindparam("ids", expanding=True))

    claimed = [r[0] for r in conn.execute(sel, {"ids": ids}).all()]
    return claimed

# 2
def run_once():
    '''
        main 루프 안에서 주기적으로 실행
        알림 대상이 될 victim을 DB에 예약
        아직 안보낸 알림(pending)을 선점(claim)
        DB 트랜잭션 안에서 실행 -> 경쟁 조건 방지 
    '''
    with engine.begin() as conn:
        inserted = reserve_jobs(conn)
        if inserted:
            print(f"[notifier] reserved {inserted} new jobs")
            
        claimed = claim_pending(conn)
        
    if not claimed:
        print("[notifier] nothing to enqueue")
        return
    
    # 5
    # DB에서 status == queued 처리, 실제 전송은 celery worker에게 위임.
    for job_id in claimed:
        send_notification.delay(job_id)  # celery task 호출해서 Redis 브로커에 job_id를 전달하면 worker는 DB에서 job 조회, victime 정보 조회하여 텔레그램에 알람을 전송하게 됨
        print(f"[notifier] enqueud job_id={job_id}")

# 1        
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