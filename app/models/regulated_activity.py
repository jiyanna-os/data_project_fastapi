from sqlalchemy import Column, String, Integer, ForeignKey
from sqlalchemy.orm import relationship
from app.core.database import Base


class RegulatedActivity(Base):
    __tablename__ = "regulated_activities"

    activity_id = Column(Integer, primary_key=True, autoincrement=True)
    activity_name = Column(String(255), unique=True, nullable=False)

    # Relationships
    locations = relationship("LocationRegulatedActivity", back_populates="activity")


class LocationRegulatedActivity(Base):
    __tablename__ = "location_regulated_activities"

    location_id = Column(String(20), ForeignKey("locations.location_id"), primary_key=True)
    activity_id = Column(Integer, ForeignKey("regulated_activities.activity_id"), primary_key=True)

    # Relationships
    location = relationship("Location", back_populates="regulated_activities")
    activity = relationship("RegulatedActivity", back_populates="locations")