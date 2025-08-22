from sqlalchemy import Column, String, Integer, Date, ForeignKey, Boolean, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.core.database import Base


class MonthlySnapshot(Base):
    __tablename__ = "monthly_snapshots"

    snapshot_id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_date = Column(Date, nullable=False, unique=True)
    file_name = Column(String(255))
    created_at = Column(DateTime, default=func.now())

    # Relationships
    location_data = relationship("LocationSnapshotData", back_populates="snapshot")


class LocationSnapshotData(Base):
    __tablename__ = "location_snapshot_data"

    location_id = Column(String(20), ForeignKey("locations.location_id"), primary_key=True)
    snapshot_id = Column(Integer, ForeignKey("monthly_snapshots.snapshot_id"), primary_key=True)
    is_active = Column(Boolean, default=True)
    latest_rating = Column(String(20))
    publication_date = Column(Date)
    is_dormant = Column(Boolean, default=False)
    care_homes_beds = Column(Integer)

    # Relationships
    location = relationship("Location", back_populates="snapshot_data")
    snapshot = relationship("MonthlySnapshot", back_populates="location_data")