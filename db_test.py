#import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# 🔐 DB 접속 정보
HOST = ""
PORT = 0
DB   = ""
USER = ""
PW   = ""

dsn = f"mysql+pymysql://{USER}:{quote_plus(PW)}@{HOST}:{PORT}/{DB}"
engine = create_engine(dsn, pool_pre_ping=True)
#MYSQL_DSN = os.environ["MYSQL_DSN"]
engine = create_engine(dsn,pool_pre_ping=True)

# 데이터베이스 테이블 정보 조회
with engine.connect() as conn:
    print("victims table")
    cols = conn.execute(text("DESCRIBE victims")).fetchall()

for c in cols:
    print(c)

with engine.connect() as conn:
    print("notifications_victims table")
    cols = conn.execute(text("DESCRIBE notifications_victims")).fetchall()

for k in cols:
    print(k)
    
with engine.connect() as conn:
    # 연결확인
    print("DB CONNECT ok:", conn.execute(text("SELECT 1")).scalar())
    
    # victims 테이블 조회
    rows = conn.execute(text("""
        SELECT `data_key`, company_name, leaked_date
        FROM victims
        ORDER BY `data_key` DESC
        LIMIT 5                             
    """)).mappings().all()
