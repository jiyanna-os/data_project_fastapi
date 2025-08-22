from pydantic import BaseModel
from typing import Optional
from datetime import date
from decimal import Decimal


class ProviderBase(BaseModel):
    provider_name: str
    brand_id: Optional[str] = None
    hsca_start_date: Optional[date] = None
    companies_house_number: Optional[str] = None
    charity_number: Optional[int] = None
    type_sector: Optional[str] = None
    inspection_directorate: Optional[str] = None
    primary_inspection_category: Optional[str] = None
    ownership_type: Optional[str] = None
    telephone_number: Optional[int] = None
    web_address: Optional[str] = None
    street_address: Optional[str] = None
    address_line_2: Optional[str] = None
    city: Optional[str] = None
    county: Optional[str] = None
    postal_code: Optional[str] = None
    paf_id: Optional[int] = None
    uprn_id: Optional[int] = None
    local_authority: Optional[str] = None
    region: Optional[str] = None
    nhs_region: Optional[str] = None
    latitude: Optional[Decimal] = None
    longitude: Optional[Decimal] = None
    parliamentary_constituency: Optional[str] = None
    nominated_individual_name: Optional[str] = None
    main_partner_name: Optional[str] = None


class ProviderCreate(ProviderBase):
    provider_id: str


class Provider(ProviderBase):
    provider_id: str

    class Config:
        from_attributes = True