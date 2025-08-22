from sqlalchemy import Column, String, Integer, DateTime, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.core.database import Base


class DataPeriod(Base):
    __tablename__ = "data_periods"

    period_id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)  # 1-12
    file_name = Column(String(255))
    created_at = Column(DateTime, default=func.now())
    
    # Ensure unique year/month combinations
    __table_args__ = (
        UniqueConstraint('year', 'month', name='uq_year_month'),
    )

    # Relationships
    location_data = relationship("LocationPeriodData", back_populates="data_period")