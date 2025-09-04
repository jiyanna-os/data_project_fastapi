from sqlalchemy import Column, String, Integer, BigInteger, DateTime, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.core.database import Base


class DataPeriod(Base):
    __tablename__ = "data_periods"

    period_id = Column(BigInteger, primary_key=True, autoincrement=True)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)  # 1-12
    file_name = Column(String)
    created_at = Column(DateTime, default=func.now())
    
    # Ensure unique year/month combinations
    __table_args__ = (
        UniqueConstraint('year', 'month', name='uq_year_month'),
    )

    # Relationships
    location_data = relationship("LocationPeriodData", back_populates="data_period")
    # location_activity_flags relationship removed - table no longer used
    location_regulated_activities = relationship("LocationRegulatedActivity", back_populates="data_period")
    location_service_types = relationship("LocationServiceType", back_populates="data_period")
    location_service_user_bands = relationship("LocationServiceUserBand", back_populates="data_period")
    dual_registrations = relationship("DualRegistration", back_populates="data_period")
    provider_brands = relationship("ProviderBrand", back_populates="data_period")