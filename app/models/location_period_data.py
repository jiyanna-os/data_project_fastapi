from sqlalchemy import Column, String, Date, Integer, BigInteger, ForeignKey, Boolean, UniqueConstraint, DECIMAL
from sqlalchemy.orm import relationship
from app.core.database import Base


class LocationPeriodData(Base):
    __tablename__ = "location_period_data"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    location_id = Column(String, ForeignKey("locations.location_id"), nullable=False)
    period_id = Column(BigInteger, ForeignKey("data_periods.period_id"), nullable=False)
    
    # Time-varying location status fields
    is_dormant = Column(Boolean, default=False)
    is_active = Column(Boolean, default=True)
    
    # Care home specific fields (time-varying)
    is_care_home = Column(Boolean, default=False) 
    care_homes_beds = Column(Integer)
    
    # Management and registration (time-varying)
    registered_manager = Column(String)
    
    # Rating information (time-varying)
    latest_overall_rating = Column(String)
    publication_date = Column(Date)
    is_inherited_rating = Column(Boolean, default=False)
    
    # Additional time-varying fields from your target data
    location_ownership_type = Column(String)
    nominated_individual_name = Column(String)
    main_partner_name = Column(String)
    
    # Inspection and compliance status fields
    current_inspection_status = Column(String)
    enforcement_actions = Column(String)
    compliance_status = Column(String)
    
    # Service delivery status
    service_capacity = Column(Integer)
    occupancy_rate = Column(DECIMAL)
    
    # Ensure unique location per period
    __table_args__ = (
        UniqueConstraint('location_id', 'period_id', name='uq_location_period'),
    )

    # Relationships
    location = relationship("Location", back_populates="period_data")
    data_period = relationship("DataPeriod", back_populates="location_data")