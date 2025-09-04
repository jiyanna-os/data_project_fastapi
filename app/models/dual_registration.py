from sqlalchemy import Column, String, Boolean, Date, BigInteger, ForeignKey
from sqlalchemy.orm import relationship
from app.core.database import Base


class DualRegistration(Base):
    __tablename__ = "dual_registrations"

    # Composite primary key
    location_id = Column(String, ForeignKey("locations.location_id"), primary_key=True)
    linked_organisation_id = Column(String, ForeignKey("locations.location_id"), primary_key=True)
    period_id = Column(BigInteger, ForeignKey("data_periods.period_id"), primary_key=True)
    
    # Additional fields from dual registration sheet
    relationship_type = Column(String, nullable=True)
    relationship_start_date = Column(Date, nullable=True)
    is_primary = Column(Boolean, default=False)
    
    # Relationships to Location model
    location = relationship("Location", foreign_keys=[location_id], back_populates="dual_registrations_as_location")
    linked_organisation = relationship("Location", foreign_keys=[linked_organisation_id], back_populates="dual_registrations_as_linked_org")
    data_period = relationship("DataPeriod", back_populates="dual_registrations")

    def __repr__(self):
        return f"<DualRegistration(location_id='{self.location_id}', linked_org='{self.linked_organisation_id}', period_id='{self.period_id}')>"