from sqlalchemy import (
    Column, Integer, String, Text, DateTime,
    ForeignKey, CheckConstraint
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import datetime

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    user_id       = Column(Integer, primary_key=True)
    first_name    = Column(String(100), nullable=False)
    last_name     = Column(String(100), nullable=False)
    email         = Column(String(255), nullable=False, unique=True)
    phone         = Column(String(20))
    registered_at = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)

    enrollments   = relationship("Enrollment", back_populates="user")
    traffic       = relationship("UserTraffic", back_populates="user")


class SalesManager(Base):
    __tablename__ = 'sales_managers'
    manager_id = Column(Integer, primary_key=True)
    first_name = Column(String(100), nullable=False)
    last_name  = Column(String(100), nullable=False)
    email      = Column(String(255), nullable=False, unique=True)
    hired_at   = Column(DateTime, nullable=False)

    sales      = relationship("Sale", back_populates="manager")


class Course(Base):
    __tablename__ = 'courses'
    course_id   = Column(Integer, primary_key=True)
    title       = Column(String(255), nullable=False)
    subject     = Column(String(100), nullable=False)
    description = Column(Text)
    price_cents = Column(Integer, nullable=False)
    created_at  = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)

    __table_args__ = (
        CheckConstraint('price_cents >= 0', name='price_non_negative'),
    )

    enrollments = relationship("Enrollment", back_populates="course")


class Enrollment(Base):
    __tablename__ = 'enrollments'

    enrollment_id = Column(Integer, primary_key=True)
    user_id       = Column(Integer, ForeignKey('users.user_id'), nullable=False)
    course_id     = Column(Integer, ForeignKey('courses.course_id'), nullable=False)
    enrolled_at   = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)

    status = Column(
        String(20),
        CheckConstraint(
            "status IN ('active','completed','cancelled')",
            name='valid_status'
        ),
        nullable=False
    )

    user   = relationship("User", back_populates="enrollments")
    course = relationship("Course", back_populates="enrollments")
    sale   = relationship("Sale", back_populates="enrollment", uselist=False)



class Sale(Base):
    __tablename__ = 'sales'
    sale_id       = Column(Integer, primary_key=True)
    enrollment_id = Column(Integer, ForeignKey('enrollments.enrollment_id'), nullable=False, unique=True)
    manager_id    = Column(Integer, ForeignKey('sales_managers.manager_id'), nullable=False)
    sale_date     = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
    cost_in_rubbles  = Column(Integer, nullable=False)

    __table_args__ = (
        CheckConstraint('cost_in_rubbles >= 0', name='amount_non_negative'),
    )

    enrollment = relationship("Enrollment", back_populates="sale")
    manager    = relationship("SalesManager", back_populates="sales")


class TrafficSource(Base):
    __tablename__ = 'traffic_sources'
    source_id = Column(Integer, primary_key=True)
    name      = Column(String(100), nullable=False)   # e.g. 'Telegram', 'VK'
    channel   = Column(String(100), nullable=False)   # e.g. 'social', 'ads'
    details   = Column(Text)

    referrals = relationship("UserTraffic", back_populates="source")


class UserTraffic(Base):
    __tablename__ = 'user_traffic'
    user_id       = Column(Integer, ForeignKey('users.user_id'),   primary_key=True)
    source_id     = Column(Integer, ForeignKey('traffic_sources.source_id'), primary_key=True)
    referred_at   = Column(DateTime, default=datetime.datetime.utcnow, nullable=False, primary_key=True)
    campaign_code = Column(String(50))

    user   = relationship("User", back_populates="traffic")
    source = relationship("TrafficSource", back_populates="referrals")
