from sqlalchemy import Column, String, Date, BigInteger, Integer, DECIMAL, ForeignKey, Boolean
from sqlalchemy.orm import relationship
from app.core.database import Base


class Location(Base):
    __tablename__ = "locations"

    # Primary identification
    location_id = Column(String(20), primary_key=True)
    provider_id = Column(String(20), ForeignKey("providers.provider_id"), nullable=False)
    location_name = Column(String(255), nullable=False)
    
    # Location static information
    location_hsca_start_date = Column(Date)
    location_ods_code = Column(String(10))
    location_telephone_number = Column(String(20))
    location_web_address = Column(String(255))
    location_type_sector = Column(String(50))
    location_inspection_directorate = Column(String(100))
    location_primary_inspection_category = Column(String(100))
    location_region = Column(String(100))
    location_nhs_region = Column(String(100))
    location_local_authority = Column(String(100))
    location_onspd_ccg_code = Column(String(10))
    location_onspd_ccg = Column(String(255))
    location_commissioning_ccg_code = Column(String(10))
    location_commissioning_ccg = Column(String(255))
    
    # Location address
    location_street_address = Column(String(255))
    location_address_line_2 = Column(String(255))
    location_city = Column(String(100))
    location_county = Column(String(100))
    location_postal_code = Column(String(10))
    location_paf_id = Column(String(20))
    location_uprn_id = Column(String(20))
    location_latitude = Column(DECIMAL(9, 6))
    location_longitude = Column(DECIMAL(9, 6))
    location_parliamentary_constituency = Column(String(150))
    
    # Location additional fields
    location_also_known_as = Column(String(255))
    location_specialisms = Column(String(500))
    location_web_address = Column(String(255))
    is_dual_registered = Column(Boolean, default=False)
    primary_id = Column(String(20))
    dual_location_id = Column(String(20))

    # Relationships
    provider = relationship("Provider", back_populates="locations")
    period_data = relationship("LocationPeriodData", back_populates="location")
    regulated_activities = relationship("LocationRegulatedActivity", back_populates="location")
    service_types = relationship("LocationServiceType", back_populates="location")
    service_user_bands = relationship("LocationServiceUserBand", back_populates="location")
    snapshot_data = relationship("LocationSnapshotData", back_populates="location")