# clear_db.py

from sqlalchemy import create_engine, text

# 1) point this at your database
DATABASE_URL = "postgresql+psycopg2://postgres:admin@localhost:5432/bi_project"
engine = create_engine(DATABASE_URL, echo=False)

# 2) run the truncate
with engine.begin() as conn:
    conn.execute(text("""
        TRUNCATE TABLE
          user_traffic,
          sales,
          enrollments,
          traffic_sources,
          sales_managers,
          courses,
          users
        RESTART IDENTITY
        CASCADE;
    """))
print("✅ All tables cleared and sequences reset.")
