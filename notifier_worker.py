# notifier_worker.py
import os
import requests
from dotenv import load_dotenv
from celery import Celery
from sqlalchemy import text
from db_info import make_engine

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
TG_BOT_TOKEN = os.environ["TG_BOT_TOKEN"]

celery_app = Celery("notifier_worker", broker=REDIS_URL, backend=REDIS_URL)
engine = make_engine()


def tg_send(chat_id: int, msg: str):
    r = requests.post(
        f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
        json={"chat_id": chat_id, "text": msg, "disable_web_page_preview": True},
        timeout=15,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Telegram error: {r.status_code} {r.text}")


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


@celery_app.task(bind=True, max_retries=3, default_retry_delay=30)
def send_notification(self, job_id: int):
    """
    job_id(=notifications_victims.id)를 받아서:
    - notifications_victims + victims 조인 조회
    - 텔레그램 전송
    - sent/failed 업데이트
    """
    try:
        with engine.begin() as conn:
            # 중복 전송 방지용 sending (여러 worker일 수도 있음!)
            upd = conn.execute(text("""
                UPDATE notifications_victims
                SET status='sending'
                WHERE id=:job_id AND status='queued'
            """), {"job_id": job_id})
            if getattr(upd, "rowcount", 0) == 0:
                return
            
            # 1) job + victim 데이터 조회 (queued 상태만 처리)
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
                # 이미 처리됐거나(=sent/failed) 잘못된 job_id거나 아직 queued가 아님
                return
            
            # # 중복 전송 방지용 sending (여러 worker일 수도 있음!)
            # upd = conn.execute(text("""
            #     UPDATE notifications_victims
            #     SET status='sending'
            #     WHERE id=:job_id AND status='queued'
            # """), {"job_id": job_id})
            # if getattr(upd, "rowcount", 0) == 0:
            #     return
            
            # channel_id NULL 이거나 문자열 방지
            if row["channel_id"] is None:
                raise RuntimeError("channel_id is NULL for this job")
            
            chat_id = int(row["channel_id"])
            msg = format_msg(dict(row))

            # 2) 텔레그램 전송
            tg_send(chat_id, msg)

            # 3) 성공 처리
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

        # 재시도 횟수 확인 (Celery가 self.request.retries에 현재 retry 횟수 들고있음)
        will_retry = self.request.retries < self.max_retries

        with engine.begin() as conn:
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

        if will_retry:
            raise self.retry(exc=e)
        else:
            raise