import os
from urllib.parse import quote_plus
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

def make_engine():
    host = os.environ["MYSQL_HOST"]
    port = int(os.getenv("MYSQL_PORT", "3306"))
    db   = os.getenv("MYSQL_DB", "crawler_db")
    user = os.environ["MYSQL_USER"]
    pw   = os.environ["MYSQL_PASSWORD"]

    dsn = f"mysql+pymysql://{user}:{quote_plus(pw)}@{host}:{port}/{db}"
    return create_engine(dsn, pool_pre_ping=True)
