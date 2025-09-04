from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class DataPeriodBase(BaseModel):
    year: int
    month: int
    file_name: Optional[str] = None


class DataPeriodCreate(DataPeriodBase):
    pass


class DataPeriod(DataPeriodBase):
    period_id: int
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True