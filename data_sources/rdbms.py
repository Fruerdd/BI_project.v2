from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from data_sources.model import Base

# Replace with your actual credentials:
DATABASE_URL = "postgresql+psycopg2://postgres:admin@localhost:5432/bi_project"

engine = create_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(bind=engine)

def init_db():
    """
    Creates all tables defined in models.py on the PostgreSQL database.
    """
    Base.metadata.create_all(bind=engine)

if __name__ == "__main__":
    init_db()
    print("✅ All tables created successfully.")
