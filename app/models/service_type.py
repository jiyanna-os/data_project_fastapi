from sqlalchemy import Column, String, Integer, ForeignKey
from sqlalchemy.orm import relationship
from app.core.database import Base


class ServiceType(Base):
    __tablename__ = "service_types"

    service_type_id = Column(Integer, primary_key=True, autoincrement=True)
    service_type_name = Column(String(255), unique=True, nullable=False)

    # Relationships
    locations = relationship("LocationServiceType", back_populates="service_type")


class LocationServiceType(Base):
    __tablename__ = "location_service_types"

    location_id = Column(String(20), ForeignKey("locations.location_id"), primary_key=True)
    service_type_id = Column(Integer, ForeignKey("service_types.service_type_id"), primary_key=True)

    # Relationships
    location = relationship("Location", back_populates="service_types")
    service_type = relationship("ServiceType", back_populates="locations")