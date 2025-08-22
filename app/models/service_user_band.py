from sqlalchemy import Column, String, Integer, ForeignKey
from sqlalchemy.orm import relationship
from app.core.database import Base


class ServiceUserBand(Base):
    __tablename__ = "service_user_bands"

    band_id = Column(Integer, primary_key=True, autoincrement=True)
    band_name = Column(String(255), unique=True, nullable=False)

    # Relationships
    locations = relationship("LocationServiceUserBand", back_populates="band")


class LocationServiceUserBand(Base):
    __tablename__ = "location_service_user_bands"

    location_id = Column(String(20), ForeignKey("locations.location_id"), primary_key=True)
    band_id = Column(Integer, ForeignKey("service_user_bands.band_id"), primary_key=True)

    # Relationships
    location = relationship("Location", back_populates="service_user_bands")
    band = relationship("ServiceUserBand", back_populates="locations")