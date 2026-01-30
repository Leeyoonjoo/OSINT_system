from sqlalchemy import text

with engine.connect() as conn:
    cols = conn.execute(text("DESCRIBE victims")).fetchall()

for c in cols:
    print(c)
