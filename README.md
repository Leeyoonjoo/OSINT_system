## Architecture
<img width="816" height="716" alt="image" src="https://github.com/user-attachments/assets/86e31ab0-cdbd-4b9b-abca-ce5985902684" /><br>
1. notifier_api가 주기적으로 실행한다. (DB 조회/ job 적재)<br>
2. pending job 일부 선택하여 queued로 변경한다 (선점하는 단계)<br>
3. queued된 job_id들을 Celery에 enqueue하여 send_notification.delay(job_id)<br>
4. notifier_worker가 job을 받으면 DB에서 Lock을 걸고 queued -> sending 선점한다 <br>
5. DB에서 job + victim 정보를 Join 조회해서 메시지를 생성하고 텔레그램에 전송한다.<br>
6. 결과를 DB에 기록하고 성공은 sent / 실패는 retry or failed로 처리한다.<br>
