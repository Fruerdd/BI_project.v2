#!/usr/bin/env python3
# clear_source_schema.py

from sqlalchemy import create_engine, MetaData, text

# ───────────── 1) DATABASE URL ─────────────────────────────────────────────────
DATABASE_URL = "postgresql+psycopg2://postgres:admin@localhost:5432/bi_project"

# ───────────── 2) CREATE ENGINE ─────────────────────────────────────────────────
engine = create_engine(DATABASE_URL, echo=True)

# ───────────── 3) REFLECT & DROP ALL TABLES IN `source` SCHEMA ────────────────────
metadata = MetaData(schema="source")

# Reflect loads all table definitions under schema="source"
metadata.reflect(bind=engine)

# Drop all reflected tables
metadata.drop_all(bind=engine)

# ───────────── 4) (OPTIONAL) DROP & RECREATE `source` SCHEMA ─────────────────────
# If you prefer to drop the entire schema and recreate it empty, uncomment below:
#
# with engine.begin() as conn:
#     conn.execute(text("DROP SCHEMA IF EXISTS source CASCADE"))
#     conn.execute(text("CREATE SCHEMA source"))

print("✅ All tables in schema `source` have been dropped.")
