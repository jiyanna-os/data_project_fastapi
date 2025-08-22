from sqlalchemy import Column, String
from sqlalchemy.orm import relationship
from app.core.database import Base


class Brand(Base):
    __tablename__ = "brands"

    brand_id = Column(String(10), primary_key=True)
    brand_name = Column(String(255), nullable=False)

    # Relationships
    providers = relationship("Provider", back_populates="brand")