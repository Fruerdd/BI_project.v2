
import random
import datetime
from faker import Faker
from tqdm import trange
from uuid import uuid4
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from data_sources.model import (
    Base,
    User,
    SalesManager,
    Course,
    Enrollment,
    Sale,
    TrafficSource,
    UserTraffic,
)

# ─── Configuration ────────────────────────────────────────────────────────────

DATABASE_URL = "postgresql+psycopg2://postgres:admin@localhost:5432/bi_project"
engine       = create_engine(DATABASE_URL, echo=False)
Session      = sessionmaker(bind=engine)

# ─── Bootstrap ────────────────────────────────────────────────────────────────

# 1) Create tables (if they don't yet exist)
Base.metadata.create_all(engine)

fake    = Faker()
session = Session()

# ─── 2) traffic_sources ───────────────────────────────────────────────────────

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
    session.add(TrafficSource(name=name, channel=channel, details=""))
session.commit()

all_sources = session.query(TrafficSource).all()

# ─── 3) users + user_traffic ─────────────────────────────────────────────────

USER_COUNT = 1_000_000
BATCH_SIZE = 10_000

for _ in trange(0, USER_COUNT, BATCH_SIZE, desc="Users"):
    users_batch = []
    for _ in range(BATCH_SIZE):
        first = fake.first_name()
        last  = fake.last_name()

        # UUID‐based email for guaranteed uniqueness
        email = f"{first.lower()}.{last.lower()}.{uuid4().hex[:8]}@example.com"

        raw_phone  = fake.phone_number()
        phone      = raw_phone if len(raw_phone) <= 20 else raw_phone[:20]

        users_batch.append(User(
            first_name    = first,
            last_name     = last,
            email         = email,
            phone         = phone,
            registered_at = fake.date_time_between(start_date="-2y", end_date="now")
        ))

    # Add & flush so that each User.user_id is populated
    session.add_all(users_batch)
    session.flush()

    # Build & bulk‐insert user_traffic for each user
    ut_batch = []
    for u in users_batch:
        src = random.choice(all_sources)
        ut_batch.append(UserTraffic(
            user_id       = u.user_id,
            source_id     = src.source_id,
            referred_at   = u.registered_at + datetime.timedelta(days=random.randint(0,10)),
            campaign_code = fake.lexify(text="????-2025")
        ))

    session.bulk_save_objects(ut_batch)
    session.commit()

# ─── 4) courses ───────────────────────────────────────────────────────────────

for _ in range(100):
    session.add(Course(
        title       = fake.catch_phrase().title(),
        subject     = fake.bs().split()[0].title(),
        description = fake.text(max_nb_chars=200),
        price_cents = random.randint(5_000, 50_000),
        created_at  = fake.date_time_between(start_date="-1y", end_date="now")
    ))
session.commit()

all_course_ids = [cid for (cid,) in session.query(Course.course_id).all()]

# ─── 5) sales_managers ────────────────────────────────────────────────────────

for _ in range(100):
    session.add(SalesManager(
        first_name = fake.first_name(),
        last_name  = fake.last_name(),
        # ensure unique email
        email      = f"{fake.company_email().split('@')[0]}.{uuid4().hex[:6]}@example.com",
        hired_at   = fake.date_between(start_date="-3y", end_date="today")
    ))
session.commit()

all_manager_ids = [mid for (mid,) in session.query(SalesManager.manager_id).all()]

# ─── 6) enrollments + sales ──────────────────────────────────────────────────

SALE_COUNT     = 321_584
inserted_sales = 0

# Cache all user IDs once for fast random picks
all_user_ids = [uid for (uid,) in session.query(User.user_id).all()]

for _ in trange(0, SALE_COUNT, BATCH_SIZE, desc="Sales"):
    this_batch = min(BATCH_SIZE, SALE_COUNT - inserted_sales)

    # 6a) Build & insert enrollments
    enroll_batch = []
    for _ in range(this_batch):
        uid         = random.choice(all_user_ids)
        cid         = random.choice(all_course_ids)
        enrolled_at = fake.date_time_between(start_date="-2y", end_date="now")
        status      = random.choice(["active", "completed", "cancelled"])

        enroll_batch.append(Enrollment(
            user_id     = uid,
            course_id   = cid,
            enrolled_at = enrolled_at,
            status      = status
        ))

    session.add_all(enroll_batch)
    session.flush()  # now each Enrollment.enrollment_id is populated

    # 6b) Build & bulk‐insert sales
    sale_batch = []
    for e in enroll_batch:
        sale_batch.append(Sale(
            enrollment_id = e.enrollment_id,
            manager_id    = random.choice(all_manager_ids),
            sale_date     = e.enrolled_at + datetime.timedelta(days=random.randint(0,30)),
            amount_cents  = random.randint(5_000, 50_000)
        ))

    session.bulk_save_objects(sale_batch)
    session.commit()
    inserted_sales += this_batch

# ─── Wrap up ──────────────────────────────────────────────────────────────────

session.close()
print("✅ All tables seeded successfully!")
