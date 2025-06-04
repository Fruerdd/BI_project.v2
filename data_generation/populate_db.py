# ────────────────────────────────────────────────────────────────────────────────
# populate_db.py
# (unchanged; all source tables already have primary keys)
# ────────────────────────────────────────────────────────────────────────────────

#!/usr/bin/env python3
# populate_db.py

"""
Populate data into the `source` schema (rather than the default “public”).
Below are the necessary changes to ensure every table and foreign key
points at `source.<table>` instead of `public.<table>`.
"""

from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    DateTime,
    ForeignKey,
    CheckConstraint,
    create_engine,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
import datetime
import random
from faker import Faker
from tqdm import trange
from uuid import uuid4

# ─────────── 0) ENSURE “source” SCHEMA EXISTS ──────────────────────────────────
DATABASE_URL = "postgresql+psycopg2://postgres:admin@localhost:5432/bi_project"
engine = create_engine(DATABASE_URL, echo=True)
with engine.begin() as conn:
    conn.execute("CREATE SCHEMA IF NOT EXISTS source")

# ─────────── 1) DECLARATIVE BASE + SCHEMA CONFIG ──────────────────────────────
Base = declarative_base(metadata=None)


class User(Base):
    __tablename__  = "users"
    __table_args__ = {"schema": "source"}

    user_id       = Column(Integer, primary_key=True)
    first_name    = Column(String(100), nullable=False)
    last_name     = Column(String(100), nullable=False)
    email         = Column(String(255), nullable=False, unique=True)
    phone         = Column(String(20))
    registered_at = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)

    enrollments   = relationship("Enrollment", back_populates="user")
    traffic       = relationship("UserTraffic", back_populates="user")


class SalesManager(Base):
    __tablename__  = "sales_managers"
    __table_args__ = {"schema": "source"}

    manager_id = Column(Integer, primary_key=True)
    first_name = Column(String(100), nullable=False)
    last_name  = Column(String(100), nullable=False)
    email      = Column(String(255), nullable=False, unique=True)
    hired_at   = Column(DateTime, nullable=False)

    sales      = relationship("Sale", back_populates="manager")


class Course(Base):
    __tablename__  = "courses"
    __table_args__ = (
        CheckConstraint("price_in_rubbles >= 0", name="price_in_rubbles_non_negative"),
        {"schema": "source"}
    )

    course_id        = Column(Integer, primary_key=True)
    title            = Column(String(255), nullable=False)
    subject          = Column(String(100), nullable=False)
    description      = Column(Text)
    price_in_rubbles = Column(Integer, nullable=False)
    created_at       = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)

    enrollments = relationship("Enrollment", back_populates="course")


class Enrollment(Base):
    __tablename__  = "enrollments"
    __table_args__ = (
        CheckConstraint(
            "status IN ('active','completed','cancelled')",
            name="valid_status"
        ),
        {"schema": "source"}
    )

    enrollment_id = Column(Integer, primary_key=True)
    user_id       = Column(Integer, ForeignKey("source.users.user_id"), nullable=False)
    course_id     = Column(Integer, ForeignKey("source.courses.course_id"), nullable=False)
    enrolled_at   = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)

    status = Column(String(20), nullable=False)

    user   = relationship("User", back_populates="enrollments")
    course = relationship("Course", back_populates="enrollments")
    sale   = relationship("Sale", back_populates="enrollment", uselist=False)


class Sale(Base):
    __tablename__  = "sales"
    __table_args__ = (
        CheckConstraint("cost_in_rubbles >= 0", name="cost_in_rubbles_non_negative"),
        {"schema": "source"}
    )

    sale_id          = Column(Integer, primary_key=True)
    enrollment_id    = Column(Integer, ForeignKey("source.enrollments.enrollment_id"), nullable=False, unique=True)
    manager_id       = Column(Integer, ForeignKey("source.sales_managers.manager_id"), nullable=False)
    sale_date        = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
    cost_in_rubbles  = Column(Integer, nullable=False)

    enrollment = relationship("Enrollment", back_populates="sale")
    manager    = relationship("SalesManager", back_populates="sales")


class TrafficSource(Base):
    __tablename__  = "traffic_sources"
    __table_args__ = {"schema": "source"}

    source_id = Column(Integer, primary_key=True)
    name      = Column(String(100), nullable=False)
    channel   = Column(String(100), nullable=False)
    details   = Column(Text)

    referrals = relationship("UserTraffic", back_populates="source")


class UserTraffic(Base):
    __tablename__  = "user_traffic"
    __table_args__ = {"schema": "source"}

    user_id       = Column(Integer, ForeignKey("source.users.user_id"), primary_key=True)
    source_id     = Column(Integer, ForeignKey("source.traffic_sources.source_id"), primary_key=True)
    referred_at   = Column(DateTime, default=datetime.datetime.utcnow, nullable=False, primary_key=True)
    campaign_code = Column(String(50))

    user   = relationship("User", back_populates="traffic")
    source = relationship("TrafficSource", back_populates="referrals")


# ─────────── 2) SEEDING LOGIC ───────────────────────────────────────────────────

Session = sessionmaker(bind=engine)
session = Session()
fake    = Faker()

# 2.1) Create all tables under “source” schema if not exist
Base.metadata.create_all(engine)

# ─── 3) Traffic sources ────────────────────────────────────────────────────────

traffic_list = [
    ("University Ads",   "ads"),
    ("Telegram Channel", "social"),
    ("VK Group",         "social"),
    ("Official Website", "web"),
    ("HH.ru",            "job"),
    ("Avito.ru",         "ads"),
    ("Email Campaign",   "email"),
    ("Word of Mouth",    "referral"),
    ("Google Ads",       "ads"),
    ("Facebook Ads",     "ads"),
]

for name, channel in traffic_list:
    session.add(
        TrafficSource(
            name    = name,
            channel = channel,
            details = ""
        )
    )
session.commit()
all_sources = session.query(TrafficSource).all()

# ─── 4) Users + user_traffic ───────────────────────────────────────────────────

USER_COUNT = 1_000_000
BATCH_SIZE = 10_000

for _ in trange(0, USER_COUNT, BATCH_SIZE, desc="Users"):
    users_batch = []
    for _ in range(BATCH_SIZE):
        first = fake.first_name()
        last  = fake.last_name()
        email = f"{first.lower()}.{last.lower()}.{uuid4().hex[:8]}@example.com"

        raw_phone = fake.phone_number()
        phone     = raw_phone if len(raw_phone) <= 20 else raw_phone[:20]

        users_batch.append(
            User(
                first_name    = first,
                last_name     = last,
                email         = email,
                phone         = phone,
                registered_at = fake.date_time_between(start_date="-2y", end_date="now")
            )
        )

    session.add_all(users_batch)
    session.flush()

    ut_batch = []
    for u in users_batch:
        src = random.choice(all_sources)
        ut_batch.append(
            UserTraffic(
                user_id       = u.user_id,
                source_id     = src.source_id,
                referred_at   = u.registered_at + datetime.timedelta(days=random.randint(0, 10)),
                campaign_code = fake.lexify(text="????-2025")
            )
        )

    session.bulk_save_objects(ut_batch)
    session.commit()

# ─── 5) Courses ────────────────────────────────────────────────────────────────

for _ in range(100):
    session.add(
        Course(
            title            = fake.catch_phrase().title(),
            subject          = fake.bs().split()[0].title(),
            description      = fake.text(max_nb_chars=200),
            price_in_rubbles = random.randint(5_000, 50_000),
            created_at       = fake.date_time_between(start_date="-1y", end_date="now")
        )
    )
session.commit()
all_course_ids = [cid for (cid,) in session.query(Course.course_id).all()]

# ─── 6) Sales Managers ─────────────────────────────────────────────────────────

for _ in range(100):
    session.add(
        SalesManager(
            first_name = fake.first_name(),
            last_name  = fake.last_name(),
            email      = f"{fake.company_email().split('@')[0]}.{uuid4().hex[:6]}@example.com",
            hired_at   = fake.date_time_between(start_date="-3y", end_date="now")
        )
    )
session.commit()
all_manager_ids = [mid for (mid,) in session.query(SalesManager.manager_id).all()]

# ─── 7) Enrollments + Sales ────────────────────────────────────────────────────

SALE_COUNT     = 321_584
inserted_sales = 0
all_user_ids   = [uid for (uid,) in session.query(User.user_id).all()]

for _ in trange(0, SALE_COUNT, BATCH_SIZE, desc="Sales"):
    this_batch = min(BATCH_SIZE, SALE_COUNT - inserted_sales)

    enroll_batch = []
    for _ in range(this_batch):
        uid         = random.choice(all_user_ids)
        cid         = random.choice(all_course_ids)
        enrolled_at = fake.date_time_between(start_date="-2y", end_date="now")
        status      = random.choice(["active", "completed", "cancelled"])

        enroll_batch.append(
            Enrollment(
                user_id     = uid,
                course_id   = cid,
                enrolled_at = enrolled_at,
                status      = status
            )
        )

    session.add_all(enroll_batch)
    session.flush()

    sale_batch = []
    for e in enroll_batch:
        sale_batch.append(
            Sale(
                enrollment_id   = e.enrollment_id,
                manager_id      = random.choice(all_manager_ids),
                sale_date       = e.enrolled_at + datetime.timedelta(days=random.randint(0, 30)),
                cost_in_rubbles = random.randint(5_000, 50_000)
            )
        )

    session.bulk_save_objects(sale_batch)
    session.commit()
    inserted_sales += this_batch

session.close()
print("✅ All `source` tables seeded successfully!")
