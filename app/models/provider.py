from sqlalchemy import Column, String, Date, BigInteger, Integer, DECIMAL, ForeignKey
from sqlalchemy.orm import relationship
from app.core.database import Base


class Provider(Base):
    __tablename__ = "providers"

    provider_id = Column(String(20), primary_key=True)
    provider_name = Column(String(255), nullable=False)
    brand_id = Column(String(10), ForeignKey("brands.brand_id"))
    hsca_start_date = Column(Date)
    companies_house_number = Column(String(20))
    charity_number = Column(BigInteger)
    type_sector = Column(String(50))
    inspection_directorate = Column(String(100))
    primary_inspection_category = Column(String(100))
    ownership_type = Column(String(50))
    telephone_number = Column(BigInteger)
    web_address = Column(String(255))
    street_address = Column(String(255))
    address_line_2 = Column(String(255))
    city = Column(String(100))
    county = Column(String(100))
    postal_code = Column(String(10))
    paf_id = Column(BigInteger)
    uprn_id = Column(BigInteger)
    local_authority = Column(String(100))
    region = Column(String(100))
    nhs_region = Column(String(100))
    latitude = Column(DECIMAL(10, 7))
    longitude = Column(DECIMAL(10, 7))
    parliamentary_constituency = Column(String(150))
    nominated_individual_name = Column(String(255))
    main_partner_name = Column(String(255))

    # Relationships
    brand = relationship("Brand", back_populates="providers")
    locations = relationship("Location", back_populates="provider")