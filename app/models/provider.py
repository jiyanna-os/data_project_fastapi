from sqlalchemy import Column, String, Date, DECIMAL, ForeignKey
from sqlalchemy.orm import relationship
from app.core.database import Base


class Provider(Base):
    __tablename__ = "providers"

    provider_id = Column(String, primary_key=True)  # String ID from source data
    provider_name = Column(String, nullable=False)
    provider_hsca_start_date = Column(Date)
    provider_companies_house_number = Column(String)
    provider_charity_number = Column(String)
    provider_type_sector = Column(String)
    provider_inspection_directorate = Column(String)
    provider_primary_inspection_category = Column(String)
    provider_ownership_type = Column(String)
    provider_telephone_number = Column(String)
    provider_web_address = Column(String)
    provider_street_address = Column(String)
    provider_address_line_2 = Column(String)
    provider_city = Column(String)
    provider_county = Column(String)
    provider_postal_code = Column(String)
    provider_paf_id = Column(String)
    provider_uprn_id = Column(String)
    provider_local_authority = Column(String)
    provider_region = Column(String)
    provider_nhs_region = Column(String)
    provider_latitude = Column(DECIMAL)
    provider_longitude = Column(DECIMAL)
    provider_parliamentary_constituency = Column(String)
    provider_nominated_individual_name = Column(String)
    provider_nominated_individual_name_raw = Column(String)  # Raw value including * and - symbols
    provider_main_partner_name = Column(String)
    provider_main_partner_name_raw = Column(String)  # Raw value including * and - symbols

    # Relationships
    brand_affiliations = relationship("ProviderBrand", back_populates="provider")
    locations = relationship("Location", back_populates="provider")