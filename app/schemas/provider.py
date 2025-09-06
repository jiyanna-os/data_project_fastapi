from pydantic import BaseModel
from typing import Optional
from datetime import date
from decimal import Decimal


class ProviderBase(BaseModel):
    provider_name: str
    provider_hsca_start_date: Optional[date] = None
    provider_companies_house_number: Optional[str] = None
    provider_charity_number: Optional[str] = None
    provider_type_sector: Optional[str] = None
    provider_inspection_directorate: Optional[str] = None
    provider_primary_inspection_category: Optional[str] = None
    provider_ownership_type: Optional[str] = None
    provider_telephone_number: Optional[str] = None
    provider_web_address: Optional[str] = None
    provider_street_address: Optional[str] = None
    provider_address_line_2: Optional[str] = None
    provider_city: Optional[str] = None
    provider_county: Optional[str] = None
    provider_postal_code: Optional[str] = None
    provider_paf_id: Optional[str] = None
    provider_uprn_id: Optional[str] = None
    provider_local_authority: Optional[str] = None
    provider_region: Optional[str] = None
    provider_nhs_region: Optional[str] = None
    provider_latitude: Optional[Decimal] = None
    provider_longitude: Optional[Decimal] = None
    provider_parliamentary_constituency: Optional[str] = None
    provider_nominated_individual_name: Optional[str] = None
    provider_nominated_individual_name_raw: Optional[str] = None
    provider_main_partner_name: Optional[str] = None
    provider_main_partner_name_raw: Optional[str] = None


class ProviderCreate(ProviderBase):
    provider_id: str


class Provider(ProviderBase):
    provider_id: str

    class Config:
        from_attributes = True