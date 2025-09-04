from sqlalchemy import Column, String, Date, Integer, BigInteger, ForeignKey, Boolean, UniqueConstraint, DECIMAL
from sqlalchemy.orm import relationship
from app.core.database import Base


class LocationPeriodData(Base):
    __tablename__ = "location_period_data"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    location_id = Column(String, ForeignKey("locations.location_id"), nullable=False)
    period_id = Column(BigInteger, ForeignKey("data_periods.period_id"), nullable=False)
    
    # Time-varying location status fields (from sample_data.csv)
    is_dormant = Column(Boolean, default=False)  # "Dormant (Y/N)"
    is_care_home = Column(Boolean, default=False)  # "Care home?"
    care_homes_beds = Column(Integer)  # "Care homes beds"
    
    # Management (time-varying)
    registered_manager = Column(String)  # "Registered manager"
    
    # Rating information (time-varying)
    latest_overall_rating = Column(String)  # "Location Latest Overall Rating"
    publication_date = Column(Date)  # "Publication Date"
    is_inherited_rating = Column(Boolean, default=False)  # "Inherited Rating (Y/N)"
    
    # Ensure unique location per period
    __table_args__ = (
        UniqueConstraint('location_id', 'period_id', name='uq_location_period'),
    )

    # Relationships
    location = relationship("Location", back_populates="period_data")
    data_period = relationship("DataPeriod", back_populates="location_data")