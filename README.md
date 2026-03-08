## Outline
한 랜섬웨어 그룹을 타겟팅해서 그룹 활동을 추적하는 시스템입니다.
알림을 받을 수 있는 정보는 이 그룹이 블로그에 올리는 기업 유출 정보들입니다.

### Key Factor
1. 실시간 다크웹 크롤링
2. 데이터 정규화 및 저장
3. 텔레그램 알림 

## Architecture
<img width="589" height="160" alt="image" src="https://github.com/user-attachments/assets/b3624dd0-7d56-4597-a70d-40eb70a5f453" /><br>
Crawler → (insert) → MySQL → (poll) → Notifier API → (enqueue) → Queue → Notifier worker → Telegram or Discord


<img width="816" height="716" alt="image" src="https://github.com/user-attachments/assets/86e31ab0-cdbd-4b9b-abca-ce5985902684" /><br>
1. notifier_api가 주기적으로 실행한다. (DB 조회/ job 적재)<br>
2. pending job 일부 선택하여 queued로 변경한다 (선점하는 단계)<br>
3. queued된 job_id들을 Celery에 enqueue하여 send_notification.delay(job_id)<br>
4. notifier_worker가 job을 받으면 DB에서 Lock을 걸고 queued -> sending 선점한다 <br>
5. DB에서 job + victim 정보를 Join 조회해서 메시지를 생성하고 텔레그램에 전송한다.<br>
6. 결과를 DB에 기록하고 성공은 sent / 실패는 retry or failed로 처리한다.<br>
