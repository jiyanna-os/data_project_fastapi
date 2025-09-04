from sqlalchemy import Column, String
from sqlalchemy.orm import relationship
from app.core.database import Base


class Brand(Base):
    __tablename__ = "brands"

    brand_id = Column(String, primary_key=True)  # String ID from source data
    brand_name = Column(String, nullable=False)

    # Relationships
    provider_affiliations = relationship("ProviderBrand", back_populates="brand")