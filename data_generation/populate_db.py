#!/usr/bin/env python3
# populate_db.py

"""
Populate data into the `source` schema (rather than the default “public”).
Below are adjustments to ensure that:
  - Course prices & sale costs follow a log‐normal distribution (high variance).
  - Users and enrollments cluster around “recent” dates via a truncated Gaussian.
  - Traffic sources and sales managers are sampled with weights (some more popular).
  - **NEW**: Each User now has a `country` column.
  - **NEW**: Each Course now has `category_id` and `subcategory_id` FK columns.
  - **UPDATED**: Sales managers now have more diverse names and a wider spread of weights.
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
    text,               # <-- for raw SQL execution
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
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS source"))

# ─────────── 1) DECLARATIVE BASE + SCHEMA CONFIG ──────────────────────────────
Base = declarative_base()

class User(Base):
    __tablename__  = "users"
    __table_args__ = {"schema": "source"}

    user_id       = Column(Integer, primary_key=True)
    first_name    = Column(String(100), nullable=False)
    last_name     = Column(String(100), nullable=False)
    email         = Column(String(255), nullable=False, unique=True)
    phone         = Column(String(20))

    # ── ADDED: Country column ────────────────────────────────────────────────────
    country       = Column(String(100), nullable=False)                             # ← ADDED

    registered_at = Column(
        DateTime, default=datetime.datetime.utcnow, nullable=False
    )

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
        CheckConstraint(
            "price_in_rubbles >= 0", name="price_in_rubbles_non_negative"
        ),
        {"schema": "source"},
    )

    course_id        = Column(Integer, primary_key=True)
    title            = Column(String(255), nullable=False)
    subject          = Column(String(100), nullable=False)
    description      = Column(Text)
    price_in_rubbles = Column(Integer, nullable=False)
    created_at       = Column(
        DateTime, default=datetime.datetime.utcnow, nullable=False
    )

    # ── ADDED: ForeignKey to Category + SubCategory ───────────────────────────────
    category_id      = Column(
        Integer,
        ForeignKey("source.categories.category_id"),
        nullable=False
    )                                                                                # ← ADDED
    subcategory_id   = Column(
        Integer,
        ForeignKey("source.subcategories.subcategory_id"),
        nullable=False
    )                                                                                # ← ADDED

    enrollments = relationship("Enrollment", back_populates="course")


class Enrollment(Base):
    __tablename__  = "enrollments"
    __table_args__ = (
        CheckConstraint(
            "status IN ('active','completed','cancelled')",
            name="valid_status"
        ),
        {"schema": "source"},
    )

    enrollment_id = Column(Integer, primary_key=True)
    user_id       = Column(
        Integer, ForeignKey("source.users.user_id"), nullable=False
    )
    course_id     = Column(
        Integer, ForeignKey("source.courses.course_id"), nullable=False
    )
    enrolled_at   = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
    status        = Column(String(20), nullable=False)

    user   = relationship("User", back_populates="enrollments")
    course = relationship("Course", back_populates="enrollments")
    sale   = relationship("Sale", back_populates="enrollment", uselist=False)


class Sale(Base):
    __tablename__  = "sales"
    __table_args__ = (
        CheckConstraint(
            "cost_in_rubbles >= 0", name="cost_in_rubbles_non_negative"
        ),
        {"schema": "source"},
    )

    sale_id          = Column(Integer, primary_key=True)
    enrollment_id    = Column(
        Integer, ForeignKey("source.enrollments.enrollment_id"), nullable=False, unique=True
    )
    manager_id       = Column(
        Integer, ForeignKey("source.sales_managers.manager_id"), nullable=False
    )
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


# ─────────── 1.1) NEW: Category + SubCategory Models (unchanged) ───────────────
class Category(Base):
    __tablename__  = "categories"
    __table_args__ = {"schema": "source"}

    category_id = Column(Integer, primary_key=True, autoincrement=True)
    name        = Column(String(100), nullable=False, unique=True)

    subcategories = relationship("SubCategory", back_populates="category")


class SubCategory(Base):
    __tablename__  = "subcategories"
    __table_args__ = {"schema": "source"}

    subcategory_id = Column(Integer, primary_key=True, autoincrement=True)
    category_id    = Column(Integer, ForeignKey("source.categories.category_id"), nullable=False)
    name           = Column(String(100), nullable=False)

    category = relationship("Category", back_populates="subcategories")


# ─────────── 2) SEEDING LOGIC ───────────────────────────────────────────────────
Session = sessionmaker(bind=engine)
session = Session()
fake    = Faker()

# Create tables if they don’t exist yet
Base.metadata.create_all(engine)

# ─── 3) CATEGORIES + SUBCATEGORIES (unchanged) ─────────────────────────────────
category_names = [
    "Technology",
    "Business",
    "Art",
    "Science",
    "Health",
    "Finance",
    "Language",
]
for cat_name in category_names:
    session.add(Category(name=cat_name))
session.commit()

all_categories = session.query(Category).all()
for cat in all_categories:
    sub_batch = []
    for i in range(1, 6):
        sub_name = f"{cat.name} Subcat {i}"
        sub_batch.append(
            SubCategory(
                category_id = cat.category_id,
                name        = sub_name
            )
        )
    session.add_all(sub_batch)
session.commit()

# ─── 4) Traffic sources with WEIGHTS ────────────────────────────────────────────
traffic_list = [
    ("University Ads",   "ads",      5),
    ("Telegram Channel", "social",  15),
    ("VK Group",         "social",  10),
    ("Official Website", "web",     20),
    ("HH.ru",            "job",      2),
    ("Avito.ru",         "ads",      4),
    ("Email Campaign",   "email",    3),
    ("Word of Mouth",    "referral",  8),
    ("Google Ads",       "ads",     10),
    ("Facebook Ads",     "ads",      8),
]
for name, channel, weight in traffic_list:
    session.add(
        TrafficSource(
            name    = name,
            channel = channel,
            details = f"Weight={weight}"
        )
    )
session.commit()

all_sources = session.query(TrafficSource).all()
source_weights = [w for (_, _, w) in traffic_list]

# ─── 5) Users + UserTraffic with CLUSTERED “registered_at” ────────────────────
USER_COUNT = 100_000
BATCH_SIZE = 10_000

def random_recent_datetime():
    """
    Use a truncated Gaussian centered around 365 days ago, sigma=200 days.
    Clamp between 0 (today) and 730 days ago.
    """
    mu_days  = 365
    sigma    = 200
    days_ago = int(abs(random.gauss(mu_days, sigma)))
    days_ago = min(days_ago, 730)
    dt       = datetime.datetime.utcnow() - datetime.timedelta(days=days_ago)
    return dt

for _ in trange(0, USER_COUNT, BATCH_SIZE, desc="Users"):
    users_batch = []
    for _ in range(BATCH_SIZE):
        first   = fake.first_name()
        last    = fake.last_name()
        email   = f"{first.lower()}.{last.lower()}.{uuid4().hex[:8]}@example.com"
        raw_phone = fake.phone_number()
        phone   = raw_phone if len(raw_phone) <= 20 else raw_phone[:20]

        # ── ADDED: assign a random country from Faker ───────────────────────────
        country = fake.country()                                                  # ← ADDED

        users_batch.append(
            User(
                first_name    = first,
                last_name     = last,
                email         = email,
                phone         = phone,
                country       = country,                                           # ← ADDED
                registered_at = random_recent_datetime()
            )
        )

    session.add_all(users_batch)
    session.flush()

    ut_batch = []
    for u in users_batch:
        src = random.choices(all_sources, weights=source_weights, k=1)[0]
        referred_at = u.registered_at + datetime.timedelta(days=random.randint(0, 30))
        ut_batch.append(
            UserTraffic(
                user_id       = u.user_id,
                source_id     = src.source_id,
                referred_at   = referred_at,
                campaign_code = fake.lexify(text="????-2025")
            )
        )

    session.bulk_save_objects(ut_batch)
    session.commit()

# ─── 6) Courses with LOG‐NORMAL “price_in_rubbles” ─────────────────────────────
def generate_course_price():
    val = random.lognormvariate(mu=10, sigma=0.75)
    price = int(val)
    price = max(1_000, min(price, 200_000))
    return price

all_category_ids    = [c.category_id for c in session.query(Category.category_id).all()]
all_subcategory_ids = [s.subcategory_id for s in session.query(SubCategory.subcategory_id).all()]

for _ in range(100):
    # assign course to a random category & subcategory
    cat_id = random.choice(all_category_ids)                                         # ← ADDED
    sub_id = random.choice(
        [s.subcategory_id for s in session.query(SubCategory).filter_by(category_id=cat_id)]
    )                                                                               # ← ADDED

    session.add(
        Course(
            title            = fake.catch_phrase().title(),
            subject          = fake.bs().split()[0].title(),
            description      = fake.text(max_nb_chars=200),
            price_in_rubbles = generate_course_price(),
            created_at       = random_recent_datetime(),
            category_id      = cat_id,                                              # ← ADDED
            subcategory_id   = sub_id                                               # ← ADDED
        )
    )
session.commit()

all_course_ids    = [cid for (cid,) in session.query(Course.course_id).all()]

# ─── 7) Sales Managers (UPDATED) ────────────────────────────────────────────────
# Generate 100 sales managers with more diverse names and a wider weight distribution.
manager_count = 100
for _ in range(manager_count):
    full_name = fake.unique.name()  # e.g., "Ava Martinez", "Liam O'Reilly"
    # Split full name into first + last: if more than two parts, take first/last.
    parts = full_name.split()
    first = parts[0]
    last = parts[-1]
    session.add(
        SalesManager(
            first_name = first,
            last_name  = last,
            email      = f"{first.lower()}.{last.lower()}.{uuid4().hex[:6]}@example.com",
            hired_at   = random_recent_datetime()
        )
    )
session.commit()
all_manager_ids = [mid for (mid,) in session.query(SalesManager.manager_id).all()]

manager_weights = []
for idx, mid in enumerate(all_manager_ids):
    if idx == 0:
        manager_weights.append(100)      # highest‐weight “super‐seller”
    elif idx == 1:
        manager_weights.append( eighty:=80)  # second‐top
    elif idx == 2:
        manager_weights.append( sixty:=60)   # third‐top
    elif idx == 3:
        manager_weights.append( forty:=40)   # fourth
    elif idx == 4:
        manager_weights.append( twenty:=20)  # fifth
    elif 5 <= idx < 15:
        manager_weights.append(random.randint(10, 20))  # mid‐range weights
    else:
        manager_weights.append(random.randint(1, 5))    # lowest weights

# ─── 8) Enrollments + Sales with VARIABLE distributions (unchanged) ────────────
SALE_COUNT     = 95_124
inserted_sales = 0
all_user_ids   = [uid for (uid,) in session.query(User.user_id).all()]

for _ in trange(0, SALE_COUNT, BATCH_SIZE, desc="Sales"):
    this_batch = min(BATCH_SIZE, SALE_COUNT - inserted_sales)

    enroll_batch = []
    for _ in range(this_batch):
        uid = random.choice(all_user_ids)
        cid = random.choice(all_course_ids)

        enrolled_at = random_recent_datetime()
        days_since = (datetime.datetime.utcnow() - enrolled_at).days
        if days_since > 30:
            status = random.choices(
                ["completed", "cancelled", "active"],
                weights=[70, 10, 20],
                k=1
            )[0]
        else:
            status = "active"

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
        chosen_mgr = random.choices(all_manager_ids, weights=manager_weights, k=1)[0]
        sale_date  = e.enrolled_at + datetime.timedelta(days=random.randint(0, 30))
        raw_cost   = random.lognormvariate(mu=10, sigma=0.9)
        cost       = int(raw_cost)
        cost       = max(500, min(cost, 300_000))

        sale_batch.append(
            Sale(
                enrollment_id   = e.enrollment_id,
                manager_id      = chosen_mgr,
                sale_date       = sale_date,
                cost_in_rubbles = cost
            )
        )

    session.bulk_save_objects(sale_batch)
    session.commit()
    inserted_sales += this_batch

session.close()
print("✅ All `source` tables seeded successfully!")
