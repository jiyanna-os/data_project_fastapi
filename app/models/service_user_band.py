from sqlalchemy import Column, String, BigInteger, ForeignKey
from sqlalchemy.orm import relationship
from app.core.database import Base


class ServiceUserBand(Base):
    __tablename__ = "service_user_bands"

    band_id = Column(BigInteger, primary_key=True, autoincrement=True)
    band_name = Column(String, unique=True, nullable=False)

    # Relationships
    locations = relationship("LocationServiceUserBand", back_populates="band")


class LocationServiceUserBand(Base):
    __tablename__ = "location_service_user_bands"

    location_id = Column(String, ForeignKey("locations.location_id"), primary_key=True)
    band_id = Column(BigInteger, ForeignKey("service_user_bands.band_id"), primary_key=True)
    period_id = Column(BigInteger, ForeignKey("data_periods.period_id"), primary_key=True)

    # Relationships
    location = relationship("Location", back_populates="service_user_bands")
    band = relationship("ServiceUserBand", back_populates="locations")
    data_period = relationship("DataPeriod", back_populates="location_service_user_bands")