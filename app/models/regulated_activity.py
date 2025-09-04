from sqlalchemy import Column, String, BigInteger, ForeignKey
from sqlalchemy.orm import relationship
from app.core.database import Base


class RegulatedActivity(Base):
    __tablename__ = "regulated_activities"

    activity_id = Column(BigInteger, primary_key=True, autoincrement=True)
    activity_name = Column(String, unique=True, nullable=False)

    # Relationships
    locations = relationship("LocationRegulatedActivity", back_populates="activity")


class LocationRegulatedActivity(Base):
    __tablename__ = "location_regulated_activities"

    location_id = Column(String, ForeignKey("locations.location_id"), primary_key=True)
    activity_id = Column(BigInteger, ForeignKey("regulated_activities.activity_id"), primary_key=True)
    period_id = Column(BigInteger, ForeignKey("data_periods.period_id"), primary_key=True)

    # Relationships
    location = relationship("Location", back_populates="regulated_activities")
    activity = relationship("RegulatedActivity", back_populates="locations")
    data_period = relationship("DataPeriod", back_populates="location_regulated_activities")