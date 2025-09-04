from pydantic import BaseModel
from typing import Optional, List
from datetime import date
from decimal import Decimal


# Static location data (doesn't change over time)
class LocationBase(BaseModel):
    location_name: str
    provider_id: str
    location_hsca_start_date: Optional[date] = None
    location_ods_code: Optional[str] = None
    location_telephone_number: Optional[str] = None
    location_web_address: Optional[str] = None
    location_type_sector: Optional[str] = None
    location_inspection_directorate: Optional[str] = None
    location_primary_inspection_category: Optional[str] = None
    location_region: Optional[str] = None
    location_nhs_region: Optional[str] = None
    location_local_authority: Optional[str] = None
    location_onspd_ccg_code: Optional[str] = None
    location_onspd_ccg: Optional[str] = None
    location_commissioning_ccg_code: Optional[str] = None
    location_commissioning_ccg: Optional[str] = None
    location_street_address: Optional[str] = None
    location_address_line_2: Optional[str] = None
    location_city: Optional[str] = None
    location_county: Optional[str] = None
    location_postal_code: Optional[str] = None
    location_paf_id: Optional[str] = None
    location_uprn_id: Optional[str] = None
    location_latitude: Optional[Decimal] = None
    location_longitude: Optional[Decimal] = None
    location_parliamentary_constituency: Optional[str] = None
    location_also_known_as: Optional[str] = None
    location_specialisms: Optional[str] = None


# Time-varying location data (changes by period)
class LocationPeriodDataBase(BaseModel):
    location_id: str
    period_id: int
    is_dormant: Optional[bool] = False
    is_active: Optional[bool] = True
    is_care_home: Optional[bool] = False
    care_homes_beds: Optional[int] = None
    registered_manager: Optional[str] = None
    latest_overall_rating: Optional[str] = None
    publication_date: Optional[date] = None
    is_inherited_rating: Optional[bool] = False
    location_ownership_type: Optional[str] = None
    nominated_individual_name: Optional[str] = None
    main_partner_name: Optional[str] = None
    current_inspection_status: Optional[str] = None
    enforcement_actions: Optional[str] = None
    compliance_status: Optional[str] = None
    service_capacity: Optional[int] = None
    occupancy_rate: Optional[Decimal] = None


class LocationCreate(LocationBase):
    location_id: str


class Location(LocationBase):
    location_id: str

    class Config:
        from_attributes = True


class LocationPeriodDataCreate(LocationPeriodDataBase):
    pass


class LocationPeriodData(LocationPeriodDataBase):
    id: int

    class Config:
        from_attributes = True