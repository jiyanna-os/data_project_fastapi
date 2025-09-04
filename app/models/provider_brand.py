from sqlalchemy import Column, String, BigInteger, ForeignKey
from sqlalchemy.orm import relationship
from app.core.database import Base


class ProviderBrand(Base):
    __tablename__ = "provider_brands"

    provider_id = Column(String, ForeignKey("providers.provider_id"), primary_key=True)
    brand_id = Column(String, ForeignKey("brands.brand_id"), primary_key=True)
    period_id = Column(BigInteger, ForeignKey("data_periods.period_id"), primary_key=True)

    # Relationships
    provider = relationship("Provider", back_populates="brand_affiliations")
    brand = relationship("Brand", back_populates="provider_affiliations")
    data_period = relationship("DataPeriod", back_populates="provider_brands")