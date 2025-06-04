#!/usr/bin/env python3
# init_source.py

from sqlalchemy import create_engine, text
from data_sources.model import Base
import sys

# Replace with your actual credentials
DATABASE_URL = "postgresql+psycopg2://postgres:admin@localhost:5432/bi_project"

def init_db():
    engine = create_engine(DATABASE_URL, echo=True)

    # 1) Ensure the public schema exists and is first in the search_path
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS public"))
        conn.execute(text("SET search_path TO public"))

    # 2) Create all tables under public
    Base.metadata.create_all(bind=engine)
    print("✅ Source (public) tables created successfully.")

if __name__ == "__main__":
    init_db()
