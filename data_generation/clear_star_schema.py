#!/usr/bin/env python3
from sqlalchemy import create_engine, text

# 1) Update this URL with your Postgres credentials / host / port / database
DATABASE_URL = "postgresql+psycopg2://postgres:admin@localhost:5432/bi_project"

# 2) Create the engine
engine = create_engine(DATABASE_URL, echo=True)

# 3) Use TRUNCATE with CASCADE to remove all rows from each table,
#    respecting foreign‐key dependencies.
with engine.begin() as conn:
    # Always truncate fact table first, then dimensions.
    conn.execute(text("TRUNCATE TABLE star_schema.fact_sales CASCADE"))
    conn.execute(text("TRUNCATE TABLE star_schema.dim_user CASCADE"))
    conn.execute(text("TRUNCATE TABLE star_schema.dim_course CASCADE"))
    conn.execute(text("TRUNCATE TABLE star_schema.dim_traffic_source CASCADE"))
    conn.execute(text("TRUNCATE TABLE star_schema.dim_date CASCADE"))

print("✅ All tables in star_schema have been cleared.")
