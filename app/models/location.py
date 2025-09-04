from sqlalchemy import Column, String, Date, DECIMAL, ForeignKey
from sqlalchemy.orm import relationship
from app.core.database import Base


class Location(Base):
    __tablename__ = "locations"

    # Primary identification
    location_id = Column(String, primary_key=True)  # String ID from source data
    provider_id = Column(String, ForeignKey("providers.provider_id"), nullable=False)
    location_name = Column(String, nullable=False)
    
    # Location static information
    location_hsca_start_date = Column(Date)
    location_ods_code = Column(String)
    location_telephone_number = Column(String)
    location_web_address = Column(String)
    location_type_sector = Column(String)
    location_inspection_directorate = Column(String)
    location_primary_inspection_category = Column(String)
    location_region = Column(String)
    location_nhs_region = Column(String)
    location_local_authority = Column(String)
    location_onspd_ccg_code = Column(String)
    location_onspd_ccg = Column(String)
    location_commissioning_ccg_code = Column(String)
    location_commissioning_ccg = Column(String)
    
    # Location address
    location_street_address = Column(String)
    location_address_line_2 = Column(String)
    location_city = Column(String)
    location_county = Column(String)
    location_postal_code = Column(String)
    location_paf_id = Column(String)
    location_uprn_id = Column(String)
    location_latitude = Column(DECIMAL)
    location_longitude = Column(DECIMAL)
    location_parliamentary_constituency = Column(String)
    
    # Location additional fields
    location_also_known_as = Column(String)
    location_specialisms = Column(String)

    # Relationships
    provider = relationship("Provider", back_populates="locations")
    period_data = relationship("LocationPeriodData", back_populates="location")
    activity_flags = relationship("LocationActivityFlags", back_populates="location")
    regulated_activities = relationship("LocationRegulatedActivity", back_populates="location")
    service_types = relationship("LocationServiceType", back_populates="location")
    service_user_bands = relationship("LocationServiceUserBand", back_populates="location")
    
    # Dual registration relationships
    dual_registrations_as_location = relationship("DualRegistration", foreign_keys="DualRegistration.location_id", back_populates="location")
    dual_registrations_as_linked_org = relationship("DualRegistration", foreign_keys="DualRegistration.linked_organisation_id", back_populates="linked_organisation")