from pydantic import BaseModel
from typing import List, Optional


class BrandBase(BaseModel):
    brand_name: str


class BrandCreate(BrandBase):
    brand_id: str


class Brand(BrandBase):
    brand_id: str

    class Config:
        from_attributes = True