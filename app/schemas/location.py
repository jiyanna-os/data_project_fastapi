from pydantic import BaseModel
from typing import Optional
from datetime import date
from decimal import Decimal


class LocationBase(BaseModel):
    location_name: str
    provider_id: str
    hsca_start_date: Optional[date] = None
    is_dormant: bool = False
    is_care_home: bool = False
    ods_code: Optional[str] = None
    telephone_number: Optional[int] = None
    registered_manager: Optional[str] = None
    web_address: Optional[str] = None
    care_homes_beds: Optional[int] = None
    type_sector: Optional[str] = None
    inspection_directorate: Optional[str] = None
    primary_inspection_category: Optional[str] = None
    latest_overall_rating: Optional[str] = None
    publication_date: Optional[date] = None
    is_inherited_rating: bool = False
    region: Optional[str] = None
    nhs_region: Optional[str] = None
    local_authority: Optional[str] = None
    onspd_ccg_code: Optional[str] = None
    onspd_ccg: Optional[str] = None
    commissioning_ccg_code: Optional[str] = None
    commissioning_ccg: Optional[str] = None
    street_address: Optional[str] = None
    address_line_2: Optional[str] = None
    city: Optional[str] = None
    county: Optional[str] = None
    postal_code: Optional[str] = None
    paf_id: Optional[int] = None
    uprn_id: Optional[int] = None
    latitude: Optional[Decimal] = None
    longitude: Optional[Decimal] = None
    parliamentary_constituency: Optional[str] = None
    is_dual_registered: bool = False
    primary_id: Optional[str] = None


class LocationCreate(LocationBase):
    location_id: str


class Location(LocationBase):
    location_id: str

    class Config:
        from_attributes = True