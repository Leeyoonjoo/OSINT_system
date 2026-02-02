# notifier_worker.py
import os
import requests
from dotenv import load_dotenv
from celery import Celery
from sqlalchemy import text
from db_info import make_engine

# 전역 초기화
# .env 로드하고 Redis를 백엔드로 쓰는 Celery 앱 생성
# DB 연결 엔진 생성 
load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
TG_BOT_TOKEN = os.environ["TG_BOT_TOKEN"]

celery_app = Celery("notifier_worker", broker=REDIS_URL, backend=REDIS_URL)
engine = make_engine()

# 텔레그램 API 호출 - send_notification()내부에서 "전송 단계"때 호출됨.
def tg_send(chat_id: int, msg: str):
    r = requests.post(
        f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
        json={"chat_id": chat_id, "text": msg, "disable_web_page_preview": True},
        timeout=15,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Telegram error: {r.status_code} {r.text}")

# 메시지 포맷팅 - send_notification()에서 DB row를 읽어온 뒤 
def format_msg(v: dict) -> str:
    lines = [
        "🚨 Ransomware Leak Alert 🚨",
        "",
        f"• Victim: {v.get('company_name') or '(unknown)'}",
        f"• Date: {v.get('leaked_date') or '(unknown)'}",
    ]
    if v.get("country"):
        lines.append(f"• Country: {v['country']}")
    if v.get("industry"):
        lines.append(f"• Industry: {v['industry']}")
    if v.get("company_url"):
        lines.append(f"• URL: {v['company_url']}")
    lines.append("")
    lines.append(f"(job_id={v.get('job_id')}, victim_id={v.get('victim_id')})")
    return "\n".join(lines)

# poller가 send_notification.delay(job_id)를 호출하면 worker가 큐에서 가져와서 실행 
@celery_app.task(bind=True, max_retries=3, default_retry_delay=30)  
# binf=True는 self로 task context 접근 가능
# max_retries=3는 총 3번까지 재시도(재시도 횟수)
# default_retry_delay=30는 retry시 기본 30초 뒤

def send_notification(self, job_id: int):
    """
    job_id(=notifications_victims.id)를 받아서:
    - notifications_victims + victims 조인 조회
    - 텔레그램 전송
    - sent/failed 업데이트
    """
    try:
        with engine.begin() as conn:  # 1 트랜잭션 시작
            # 2 중복 전송 방지용 sending (여러 worker일 수도 있음!)
            upd = conn.execute(text("""
                UPDATE notifications_victims
                SET status='sending'
                WHERE id=:job_id AND status='queued'
            """), {"job_id": job_id})
            if getattr(upd, "rowcount", 0) == 0:  # rowcount는 이미 누가 가져갔거나(status=sending/sent/failed), 아직 queued가 아닌 상태이면 그냥 종료
                return
            
            # 3 job + victim 데이터 조회 (status가 sending 해당)
            row = conn.execute(
                text(
                    """
                    SELECT
                        n.id AS job_id,
                        n.channel_id,
                        n.victim_id,
                        v.company_name,
                        v.leaked_date,
                        v.company_url,
                        v.industry,
                        v.country
                    FROM notifications_victims n
                    JOIN victims v ON v.id = n.victim_id
                    WHERE n.id = :job_id
                      AND n.status = 'sending'
                    LIMIT 1
                    """
                ),
                {"job_id": job_id},
            ).mappings().first()

            if not row:
                return
            
            # 4 channel_id 검증
            if row["channel_id"] is None:
                raise RuntimeError("channel_id is NULL for this job")
            
            chat_id = int(row["channel_id"]) # 문자열, None 예외처리
            
            # 5 텔레그램 전송
            msg = format_msg(dict(row)) # 메시지 전송
            tg_send(chat_id, msg)

            # 6 성공 처리: sending -> sent 으로 status 변경
            conn.execute(
                text(
                    """
                    UPDATE notifications_victims
                    SET status='sent',
                        sent_at=NOW(),
                        last_error=NULL
                    WHERE id=:job_id
                    """
                ),
                {"job_id": job_id},
            )

    except Exception as e:
        err = f"{type(e).__name__}: {e}"

        will_retry = self.request.retries < self.max_retries # 아직 재시도 남아있으면 True, 아니리면 False(최종 failed됨)

        with engine.begin() as conn:
            # 실패 상태 DB 기록
            # 재시도 남음(will_retry=True) -> status="queued"
            # 재시도 끝(will_retry=False) -> status="failed"
            conn.execute(text("""
                UPDATE notifications_victims
                SET status = :status,
                    retry_count = retry_count + 1,
                    last_error = :err
                WHERE id = :job_id
            """), {
                "job_id": job_id,
                "status": "queued" if will_retry else "failed",
                "err": err[:5000],
            })

        if will_retry: # self.retry()를 raise 해야 Celery가 "이건 재시도 대상"으로 인식
            raise self.retry(exc=e)
        else:          # 재시도 없으면 그냥 예외처리돼서 task 실패
            raise