import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from app.models.brand import Brand
from app.models.provider import Provider
from app.models.location import Location
from app.models.location_period_data import LocationPeriodData
from app.models.regulated_activity import RegulatedActivity, LocationRegulatedActivity
from app.models.service_type import ServiceType, LocationServiceType
from app.models.service_user_band import ServiceUserBand, LocationServiceUserBand
from app.models.data_period import DataPeriod
from app.models.location_activity_flags import LocationActivityFlags

logger = logging.getLogger(__name__)


class CQCDataImporter:
    def __init__(self, db: Session):
        self.db = db
        self.stats = {
            "brands_created": 0,
            "providers_created": 0,
            "locations_created": 0,
            "location_period_data_created": 0,
            "location_activity_flags_created": 0,
            "activities_created": 0,
            "service_types_created": 0,
            "user_bands_created": 0,
            "periods_created": 0,
            "errors": []
        }

    def clean_value(self, value) -> Optional[str]:
        """Clean and validate string values"""
        if pd.isna(value) or value == '-' or value == '*':
            return None
        result = str(value).strip() if value else None
        # Truncate very long strings to prevent database errors
        if result and len(result) > 250:
            result = result[:250]
        return result

    def parse_date(self, date_str) -> Optional[datetime]:
        """Parse date string to datetime object with comprehensive format support"""
        if pd.isna(date_str) or date_str == '' or date_str == '-' or date_str == '*':
            return None
        
        # Handle string values
        if isinstance(date_str, str):
            # Clean the string
            date_str = date_str.strip()
            if not date_str:
                return None
                
            # Try comprehensive list of date formats commonly found in CQC data
            date_formats = [
                # ISO formats
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d",
                # UK formats (DD/MM/YYYY)
                "%d/%m/%Y",
                "%d-%m-%Y",
                "%d.%m.%Y",
                # US formats (MM/DD/YYYY)
                "%m/%d/%Y",
                "%m-%d-%Y",
                # Alternative formats
                "%d %m %Y",
                "%d %B %Y",    # 01 January 2025
                "%d %b %Y",    # 01 Jan 2025
                "%B %d, %Y",   # January 01, 2025
                "%b %d, %Y",   # Jan 01, 2025
                # Excel date formats
                "%d/%m/%y",    # 01/01/25
                "%m/%d/%y",    # 01/01/25
                "%d-%m-%y",    # 01-01-25
                "%m-%d-%y",    # 01-01-25
                # Date with time variations
                "%Y-%m-%d %H:%M",
                "%d/%m/%Y %H:%M:%S",
                "%d/%m/%Y %H:%M",
                "%m/%d/%Y %H:%M:%S",
                "%m/%d/%Y %H:%M",
                # ISO 8601 variants
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S.%f",
                # Month/Year only formats (assume 1st of month)
                "%m/%Y",       # 01/2025
                "%Y-%m",       # 2025-01
                "%B %Y",       # January 2025
                "%b %Y",       # Jan 2025
            ]
            
            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(date_str, fmt)
                    return parsed_date.date()
                except ValueError:
                    continue
            
            # Try pandas date parser as fallback
            try:
                parsed = pd.to_datetime(date_str, dayfirst=True, errors='coerce')
                if not pd.isna(parsed):
                    return parsed.date()
            except:
                pass
                
            # Log unrecognized date format for debugging
            logger.warning(f"Could not parse date: '{date_str}'")
            return None
            
        # Handle pandas Timestamp
        elif hasattr(date_str, 'to_pydatetime'):
            return date_str.to_pydatetime().date()
            
        # Handle datetime objects
        elif hasattr(date_str, 'date'):
            return date_str.date()
            
        # Handle numeric values (Excel serial dates)
        elif isinstance(date_str, (int, float)):
            try:
                # Excel epoch starts from 1900-01-01 (with 1900 leap year bug)
                # Excel serial 1 = 1900-01-01, but we need to account for the bug
                if date_str > 59:  # After Feb 28, 1900
                    excel_date = datetime(1899, 12, 30) + pd.Timedelta(days=date_str)
                else:
                    excel_date = datetime(1899, 12, 31) + pd.Timedelta(days=date_str - 1)
                return excel_date.date()
            except:
                logger.warning(f"Could not parse numeric date: {date_str}")
                return None
                
        # Unknown type
        else:
            logger.warning(f"Unknown date type: {type(date_str)} - {date_str}")
            return None

    def validate_date(self, date_value, field_name: str) -> Optional[datetime]:
        """Validate dates for logical consistency"""
        if not date_value:
            return None
            
        try:
            from datetime import date as date_class
            if isinstance(date_value, date_class):
                current_date = date_class.today()
                
                # Check for future publication dates (likely data error)
                if field_name == 'publication_date' and date_value > current_date:
                    logger.warning(f"Future publication date detected: {date_value}")
                    # Still return the date but log the warning
                    
                # Check for very old dates (likely data error)
                if date_value.year < 1900:
                    logger.warning(f"Very old date detected for {field_name}: {date_value}")
                    return None
                    
                return date_value
        except Exception as e:
            logger.warning(f"Date validation error for {field_name}: {str(e)}")
            return date_value

    def parse_boolean(self, value) -> bool:
        """Parse Y/N values to boolean"""
        if pd.isna(value):
            return False
        return str(value).upper() == 'Y'

    def parse_number(self, value) -> Optional[int]:
        """Parse numeric values for integer fields"""
        """Parse numeric values"""
        if pd.isna(value) or value == '' or value == '-':
            return None
        try:
            # Handle scientific notation and large numbers
            num = float(value)
            if num > 9223372036854775807:  # Max BigInt value
                return None
            return int(num)
        except:
            return None

    def parse_decimal(self, value) -> Optional[float]:
        """Parse decimal values"""
        if pd.isna(value) or value == '':
            return None
        try:
            return float(value)
        except:
            return None

    def parse_telephone(self, value) -> Optional[str]:
        """Parse telephone numbers with UK format validation"""
        if pd.isna(value) or value == '' or value == '-':
            return None
        try:
            # Convert to string and strip whitespace
            phone = str(value).strip()
            # Handle scientific notation from Excel
            if 'e+' in phone.lower():
                # Convert scientific notation to integer first, then string
                phone = str(int(float(phone)))
            
            # Remove non-digit characters
            import re
            phone = re.sub(r'[^\d]', '', phone)
            
            if not phone:
                return None
                
            # UK phone number validation and formatting
            if len(phone) == 11 and phone.startswith('0'):
                # Standard UK number: 01709789790 -> 01709789790
                return phone
            elif len(phone) == 10 and not phone.startswith('0'):
                # Missing leading zero: 1709789790 -> 01709789790
                return '0' + phone
            elif len(phone) > 11:
                # Too many digits - truncate from right (assuming extra digits added)
                return phone[:11]
            elif len(phone) < 10:
                # Too few digits - log warning but keep
                logger.warning(f"Short phone number: {phone}")
                return phone
            else:
                return phone
        except Exception as e:
            logger.warning(f"Could not parse phone number: {value} - {str(e)}")
            return None

    def get_or_create_brand(self, brand_id: str, brand_name: str) -> Optional[Brand]:
        """Get existing brand or create new one"""
        if not brand_id or brand_id == '-':
            return None
            
        brand = self.db.query(Brand).filter(Brand.brand_id == brand_id).first()
        if not brand:
            brand = Brand(
                brand_id=brand_id,
                brand_name=brand_name or f"Brand {brand_id}"
            )
            try:
                self.db.add(brand)
                self.db.commit()
                self.stats["brands_created"] += 1
                logger.info(f"Created brand: {brand_id}")
            except IntegrityError:
                self.db.rollback()
                brand = self.db.query(Brand).filter(Brand.brand_id == brand_id).first()
        return brand

    def get_or_create_regulated_activity(self, activity_name: str) -> RegulatedActivity:
        """Get existing regulated activity or create new one"""
        activity = self.db.query(RegulatedActivity).filter(
            RegulatedActivity.activity_name == activity_name
        ).first()
        
        if not activity:
            activity = RegulatedActivity(activity_name=activity_name)
            try:
                self.db.add(activity)
                self.db.commit()
                self.stats["activities_created"] += 1
            except IntegrityError:
                self.db.rollback()
                activity = self.db.query(RegulatedActivity).filter(
                    RegulatedActivity.activity_name == activity_name
                ).first()
        return activity

    def get_or_create_data_period(self, year: int, month: int, file_name: str) -> DataPeriod:
        """Get existing data period or create new one"""
        period = self.db.query(DataPeriod).filter(
            DataPeriod.year == year,
            DataPeriod.month == month
        ).first()
        
        if not period:
            period = DataPeriod(
                year=year,
                month=month,
                file_name=file_name
            )
            try:
                self.db.add(period)
                self.db.commit()
                self.stats["periods_created"] += 1
                logger.info(f"Created data period: {year}-{month:02d}")
            except IntegrityError:
                self.db.rollback()
                period = self.db.query(DataPeriod).filter(
                    DataPeriod.year == year,
                    DataPeriod.month == month
                ).first()
        return period

    def get_or_create_service_type(self, service_name: str) -> ServiceType:
        """Get existing service type or create new one"""
        service_type = self.db.query(ServiceType).filter(
            ServiceType.service_type_name == service_name
        ).first()
        
        if not service_type:
            service_type = ServiceType(service_type_name=service_name)
            try:
                self.db.add(service_type)
                self.db.commit()
                self.stats["service_types_created"] += 1
            except IntegrityError:
                self.db.rollback()
                service_type = self.db.query(ServiceType).filter(
                    ServiceType.service_type_name == service_name
                ).first()
        return service_type

    def get_or_create_service_user_band(self, band_name: str) -> ServiceUserBand:
        """Get existing service user band or create new one"""
        band = self.db.query(ServiceUserBand).filter(
            ServiceUserBand.band_name == band_name
        ).first()
        
        if not band:
            band = ServiceUserBand(band_name=band_name)
            try:
                self.db.add(band)
                self.db.commit()
                self.stats["user_bands_created"] += 1
            except IntegrityError:
                self.db.rollback()
                band = self.db.query(ServiceUserBand).filter(
                    ServiceUserBand.band_name == band_name
                ).first()
        return band

    def create_provider(self, row: pd.Series) -> Optional[Provider]:
        """Create provider from row data"""
        provider_id = self.clean_value(row.get('Provider ID'))
        if not provider_id:
            return None

        # Check if provider already exists
        existing_provider = self.db.query(Provider).filter(Provider.provider_id == provider_id).first()
        if existing_provider:
            return existing_provider

        # Get or create brand
        brand_id = self.clean_value(row.get('Brand ID'))
        brand_name = self.clean_value(row.get('Brand Name'))
        brand = None
        if brand_id and brand_id != '-':
            brand = self.get_or_create_brand(brand_id, brand_name)

        provider = Provider(
            provider_id=provider_id,
            provider_name=self.clean_value(row.get('Provider Name')) or f"Provider {provider_id}",
            brand_id=brand.brand_id if brand else None,
            hsca_start_date=self.parse_date(row.get('Provider HSCA start date')),
            companies_house_number=self.clean_value(row.get('Provider Companies House Number')),
            charity_number=self.parse_number(row.get('Provider Charity Number')),
            type_sector=self.clean_value(row.get('Provider Type/Sector')),
            inspection_directorate=self.clean_value(row.get('Provider Inspection Directorate')),
            primary_inspection_category=self.clean_value(row.get('Provider Primary Inspection Category')),
            ownership_type=self.clean_value(row.get('Provider Ownership Type')),
            telephone_number=self.parse_telephone(row.get('Provider Telephone Number')),
            web_address=self.clean_value(row.get('Provider Web Address')),
            street_address=self.clean_value(row.get('Provider Street Address')),
            address_line_2=self.clean_value(row.get('Provider Address Line 2')),
            city=self.clean_value(row.get('Provider City')),
            county=self.clean_value(row.get('Provider County')),
            postal_code=self.clean_value(row.get('Provider Postal Code')),
            paf_id=self.clean_value(row.get('Provider PAF ID')),
            uprn_id=self.clean_value(row.get('Provider UPRN ID')),
            local_authority=self.clean_value(row.get('Provider Local Authority')),
            region=self.clean_value(row.get('Provider Region')),
            nhs_region=self.clean_value(row.get('Provider NHS Region')),
            latitude=self.parse_decimal(row.get('Provider Latitude')),
            longitude=self.parse_decimal(row.get('Provider Longitude')),
            parliamentary_constituency=self.clean_value(row.get('Provider Parliamentary Constituency')),
            nominated_individual_name=self.clean_value(row.get('Provider Nominated Individual Name')),
            main_partner_name=self.clean_value(row.get('Provider Main Partner Name'))
        )

        try:
            self.db.add(provider)
            self.db.commit()
            self.stats["providers_created"] += 1
            logger.info(f"Created provider: {provider_id}")
            return provider
        except IntegrityError as e:
            self.db.rollback()
            self.stats["errors"].append(f"Provider {provider_id}: {str(e)}")
            return self.db.query(Provider).filter(Provider.provider_id == provider_id).first()

    def get_or_create_location(self, row: pd.Series, provider: Provider) -> Optional[Location]:
        """Get existing location or create new one (static data only)"""
        location_id = self.clean_value(row.get('Location ID'))
        if not location_id:
            return None

        # Check if location already exists
        existing_location = self.db.query(Location).filter(Location.location_id == location_id).first()
        if existing_location:
            return existing_location

        # Create new location with static data only
        location = Location(
            location_id=location_id,
            provider_id=provider.provider_id,
            location_name=self.clean_value(row.get('Location Name')) or f"Location {location_id}",
            location_hsca_start_date=self.parse_date(row.get('Location HSCA start date')),
            location_ods_code=self.clean_value(row.get('Location ODS Code')),
            location_telephone_number=self.parse_telephone(row.get('Location Telephone Number')),
            location_web_address=self.clean_value(row.get('Location Web Address')),
            location_type_sector=self.clean_value(row.get('Location Type/Sector')),
            location_inspection_directorate=self.clean_value(row.get('Location Inspection Directorate')),
            location_primary_inspection_category=self.clean_value(row.get('Location Primary Inspection Category')),
            location_region=self.clean_value(row.get('Location Region')),
            location_nhs_region=self.clean_value(row.get('Location NHS Region')),
            location_local_authority=self.clean_value(row.get('Location Local Authority')),
            location_onspd_ccg_code=self.clean_value(row.get('Location ONSPD CCG Code')),
            location_onspd_ccg=self.clean_value(row.get('Location ONSPD CCG')),
            location_commissioning_ccg_code=self.clean_value(row.get('Location Commissioning CCG Code')),
            location_commissioning_ccg=self.clean_value(row.get('Location Commissioning CCG')),
            location_street_address=self.clean_value(row.get('Location Street Address')),
            location_address_line_2=self.clean_value(row.get('Location Address Line 2')),
            location_city=self.clean_value(row.get('Location City')),
            location_county=self.clean_value(row.get('Location County')),
            location_postal_code=self.clean_value(row.get('Location Postal Code')),
            location_paf_id=self.clean_value(row.get('Location PAF ID')),
            location_uprn_id=self.clean_value(row.get('Location UPRN ID')),
            location_latitude=self.parse_decimal(row.get('Location Latitude')),
            location_longitude=self.parse_decimal(row.get('Location Longitude')),
            location_parliamentary_constituency=self.clean_value(row.get('Location Parliamentary Constituency')),
            location_also_known_as=self.clean_value(row.get('Location Also Known As')),
            location_specialisms=self.clean_value(row.get('Location Specialisms')),
            is_dual_registered=self.parse_boolean(row.get('Location Dual Registered')),
            primary_id=self.clean_value(row.get('Primary ID (Dual registration locations)'))
        )

        try:
            self.db.add(location)
            self.db.commit()
            self.stats["locations_created"] += 1
            logger.info(f"Created location: {location_id}")
            return location
        except IntegrityError as e:
            self.db.rollback()
            self.stats["errors"].append(f"Location {location_id}: {str(e)}")
            return self.db.query(Location).filter(Location.location_id == location_id).first()

    def create_location_period_data(self, location: Location, row: pd.Series, data_period: DataPeriod) -> Optional[LocationPeriodData]:
        """Create time-varying data for a location in a specific period"""
        # Check if period data already exists
        existing_period_data = self.db.query(LocationPeriodData).filter(
            LocationPeriodData.location_id == location.location_id,
            LocationPeriodData.period_id == data_period.period_id
        ).first()
        
        if existing_period_data:
            return existing_period_data
        
        # Create new period data
        period_data = LocationPeriodData(
            location_id=location.location_id,
            period_id=data_period.period_id,
            is_dormant=self.parse_boolean(row.get('Dormant (Y/N)')),
            is_care_home=self.parse_boolean(row.get('Care home?')),
            registered_manager=self.clean_value(row.get('Registered manager')),
            care_homes_beds=self.parse_number(row.get('Care homes beds')),
            latest_overall_rating=self.clean_value(row.get('Location Latest Overall Rating')),
            publication_date=self.validate_date(self.parse_date(row.get('Publication Date')), 'publication_date'),
            is_inherited_rating=self.parse_boolean(row.get('Inherited Rating (Y/N)'))
        )
        
        try:
            self.db.add(period_data)
            self.db.commit()
            self.stats["location_period_data_created"] += 1
            return period_data
        except IntegrityError as e:
            self.db.rollback()
            self.stats["errors"].append(f"Location period data {location.location_id}-{data_period.period_id}: {str(e)}")
            return self.db.query(LocationPeriodData).filter(
                LocationPeriodData.location_id == location.location_id,
                LocationPeriodData.period_id == data_period.period_id
            ).first()

    def add_location_activities(self, location: Location, row: pd.Series):
        """Add regulated activities for a location"""
        activity_columns = [col for col in row.index if col.startswith('Regulated activity -')]
        
        for col in activity_columns:
            if self.parse_boolean(row[col]):
                activity_name = col.replace('Regulated activity - ', '')
                activity = self.get_or_create_regulated_activity(activity_name)
                
                # Check if relationship already exists
                existing = self.db.query(LocationRegulatedActivity).filter(
                    LocationRegulatedActivity.location_id == location.location_id,
                    LocationRegulatedActivity.activity_id == activity.activity_id
                ).first()
                
                if not existing:
                    location_activity = LocationRegulatedActivity(
                        location_id=location.location_id,
                        activity_id=activity.activity_id
                    )
                    self.db.add(location_activity)

    def add_location_service_types(self, location: Location, row: pd.Series):
        """Add service types for a location"""
        service_columns = [col for col in row.index if col.startswith('Service type -')]
        
        for col in service_columns:
            if self.parse_boolean(row[col]):
                service_name = col.replace('Service type - ', '')
                service_type = self.get_or_create_service_type(service_name)
                
                # Check if relationship already exists
                existing = self.db.query(LocationServiceType).filter(
                    LocationServiceType.location_id == location.location_id,
                    LocationServiceType.service_type_id == service_type.service_type_id
                ).first()
                
                if not existing:
                    location_service = LocationServiceType(
                        location_id=location.location_id,
                        service_type_id=service_type.service_type_id
                    )
                    self.db.add(location_service)

    def add_location_user_bands(self, location: Location, row: pd.Series):
        """Add service user bands for a location"""
        band_columns = [col for col in row.index if col.startswith('Service user band -')]
        
        for col in band_columns:
            if self.parse_boolean(row[col]):
                band_name = col.replace('Service user band - ', '')
                band = self.get_or_create_service_user_band(band_name)
                
                # Check if relationship already exists
                existing = self.db.query(LocationServiceUserBand).filter(
                    LocationServiceUserBand.location_id == location.location_id,
                    LocationServiceUserBand.band_id == band.band_id
                ).first()
                
                if not existing:
                    location_band = LocationServiceUserBand(
                        location_id=location.location_id,
                        band_id=band.band_id
                    )
                    self.db.add(location_band)

    def create_location_activity_flags(self, location: Location, row: pd.Series, data_period: DataPeriod) -> Optional[LocationActivityFlags]:
        """Create activity flags for a location in a specific period"""
        # Check if flags already exist
        existing_flags = self.db.query(LocationActivityFlags).filter(
            LocationActivityFlags.location_id == location.location_id,
            LocationActivityFlags.period_id == data_period.period_id
        ).first()
        
        if existing_flags:
            return existing_flags
        
        # Create new activity flags record
        activity_flags = LocationActivityFlags(
            location_id=location.location_id,
            period_id=data_period.period_id,
            
            # Regulated Activities - Complete mapping
            accommodation_nursing_personal_care=self.parse_boolean(row.get('Regulated activity - Accommodation for persons who require nursing or personal care')),
            treatment_disease_disorder_injury=self.parse_boolean(row.get('Regulated activity - Treatment of disease, disorder or injury')),
            assessment_medical_treatment=self.parse_boolean(row.get('Regulated activity - Assessment or medical treatment for persons detained under the Mental Health Act 1983')),
            surgical_procedures=self.parse_boolean(row.get('Regulated activity - Surgical procedures')),
            diagnostic_screening=self.parse_boolean(row.get('Regulated activity - Diagnostic and screening procedures')),
            management_supply_blood=self.parse_boolean(row.get('Regulated activity - Management of supply of blood and blood derived products')),
            transport_services=self.parse_boolean(row.get('Regulated activity - Transport services, triage and medical advice provided remotely')),
            maternity_midwifery=self.parse_boolean(row.get('Regulated activity - Maternity and midwifery services')),
            termination_pregnancies=self.parse_boolean(row.get('Regulated activity - Termination of pregnancies')),
            services_slimming=self.parse_boolean(row.get('Regulated activity - Services in slimming clinics')),
            nursing_care=self.parse_boolean(row.get('Regulated activity - Nursing care')),
            personal_care=self.parse_boolean(row.get('Regulated activity - Personal care')),
            accommodation_persons_detoxification=self.parse_boolean(row.get('Regulated activity - Accommodation for persons who require treatment for substance misuse')),
            family_planning=self.parse_boolean(row.get('Regulated activity - Family planning')),
            
            # Service Types - Complete mapping from CQC data
            acute_services_with_overnight_beds=self.parse_boolean(row.get('Service type - Acute services with overnight beds')),
            acute_services_without_overnight_beds=self.parse_boolean(row.get('Service type - Acute services without overnight beds / listed acute services with or without overnight beds')),
            ambulance_service=self.parse_boolean(row.get('Service type - Ambulance service')),
            blood_and_transplant_service=self.parse_boolean(row.get('Service type - Blood and Transplant service')),
            care_home_nursing=self.parse_boolean(row.get('Service type - Care home service with nursing')),
            care_home_without_nursing=self.parse_boolean(row.get('Service type - Care home service without nursing')),
            community_based_services_substance_misuse=self.parse_boolean(row.get('Service type - Community based services for people who misuse substances')),
            community_based_services_learning_disability=self.parse_boolean(row.get('Service type - Community based services for people with a learning disability')),
            community_based_services_mental_health=self.parse_boolean(row.get('Service type - Community based services for people with mental health needs')),
            community_health_care_independent_midwives=self.parse_boolean(row.get('Service type - Community health care services - Independent Midwives')),
            community_health_care_nurses_agency=self.parse_boolean(row.get('Service type - Community health care services - Nurses Agency only')),
            community_health_care=self.parse_boolean(row.get('Service type - Community healthcare service')),
            dental_service=self.parse_boolean(row.get('Service type - Dental service')),
            diagnostic_screening_service=self.parse_boolean(row.get('Service type - Diagnostic and/or screening service')),
            diagnostic_screening_single_handed_sessional=self.parse_boolean(row.get('Service type - Diagnostic and/or screening service - single handed sessional providers')),
            doctors_consultation=self.parse_boolean(row.get('Service type - Doctors consultation service')),
            doctors_treatment=self.parse_boolean(row.get('Service type - Doctors treatment service')),
            domiciliary_care=self.parse_boolean(row.get('Service type - Domiciliary care service')),
            extra_care_housing=self.parse_boolean(row.get('Service type - Extra Care housing services')),
            hospice_services=self.parse_boolean(row.get('Service type - Hospice services')),
            hospice_services_at_home=self.parse_boolean(row.get('Service type - Hospice services at home')),
            hospital_services_mental_health_learning_disabilities=self.parse_boolean(row.get('Service type - Hospital services for people with mental health needs, learning disabilities and problems with substance misuse')),
            hospital_services_acute=self.parse_boolean(row.get('Service type - Hospital services for people detained under the Mental Health Act')),
            hyperbaric_chamber=self.parse_boolean(row.get('Service type - Hyperbaric Chamber')),
            long_term_conditions=self.parse_boolean(row.get('Service type - Long term conditions services')),
            mobile_doctors=self.parse_boolean(row.get('Service type - Mobile doctors service')),
            prison_healthcare=self.parse_boolean(row.get('Service type - Prison Healthcare Services')),
            rehabilitation_services=self.parse_boolean(row.get('Service type - Rehabilitation services')),
            remote_clinical_advice=self.parse_boolean(row.get('Service type - Remote clinical advice service')),
            residential_substance_misuse_treatment=self.parse_boolean(row.get('Service type - Residential substance misuse treatment and/or rehabilitation service')),
            shared_lives=self.parse_boolean(row.get('Service type - Shared Lives')),
            specialist_college=self.parse_boolean(row.get('Service type - Specialist college service')),
            supported_living=self.parse_boolean(row.get('Service type - Supported living service')),
            urgent_care=self.parse_boolean(row.get('Service type - Urgent care services')),
            
            # Service User Bands - Complete mapping from CQC data
            children_0_18_years=self.parse_boolean(row.get('Service user band - Children 0-18 years')),
            dementia=self.parse_boolean(row.get('Service user band - Dementia')),
            learning_disabilities_autistic=self.parse_boolean(row.get('Service user band - Learning disabilities or autistic spectrum disorder')),
            mental_health_needs=self.parse_boolean(row.get('Service user band - Mental Health')),
            older_people_65_plus=self.parse_boolean(row.get('Service user band - Older People')),
            people_detained_mental_health_act=self.parse_boolean(row.get('Service user band - People detained under the Mental Health Act')),
            people_who_misuse_drugs_alcohol=self.parse_boolean(row.get('Service user band - People who misuse drugs and alcohol')),
            people_with_eating_disorder=self.parse_boolean(row.get('Service user band - People with an eating disorder')),
            physical_disability=self.parse_boolean(row.get('Service user band - Physical Disability')),
            sensory_impairment=self.parse_boolean(row.get('Service user band - Sensory Impairment')),
            whole_population=self.parse_boolean(row.get('Service user band - Whole Population')),
            younger_adults=self.parse_boolean(row.get('Service user band - Younger Adults')),
            
            # Legacy backward compatibility fields
            children_0_3_years=False,  # Map to children_0_18_years for compatibility
            children_4_12_years=False,  # Map to children_0_18_years for compatibility  
            children_13_18_years=False,  # Map to children_0_18_years for compatibility
            adults_18_65_years=self.parse_boolean(row.get('Service user band - Younger Adults')),  # Map to younger_adults
        )
        
        try:
            self.db.add(activity_flags)
            self.db.commit()
            self.stats["location_activity_flags_created"] += 1
            return activity_flags
        except IntegrityError as e:
            self.db.rollback()
            self.stats["errors"].append(f"Location activity flags {location.location_id}-{data_period.period_id}: {str(e)}")
            return self.db.query(LocationActivityFlags).filter(
                LocationActivityFlags.location_id == location.location_id,
                LocationActivityFlags.period_id == data_period.period_id
            ).first()

    def import_from_excel(self, excel_path: str, filter_care_homes: bool = None, year: int = None, month: int = None) -> Dict:
        """Import data from Excel file with optional filtering"""
        try:
            logger.info(f"Starting import from {excel_path}")
            
            # Validate year and month parameters
            if year is None or month is None:
                raise ValueError("Both year and month parameters are required")
            
            if not (1 <= month <= 12):
                raise ValueError("Month must be between 1 and 12")
                
            # Create or get data period
            file_name = excel_path.split('/')[-1]  # Extract filename
            data_period = self.get_or_create_data_period(year, month, file_name)
            logger.info(f"Using data period: {year}-{month:02d} (ID: {data_period.period_id})")
            
            # Load the main data sheet
            df = pd.read_excel(excel_path, sheet_name='HSCA_Active_Locations')
            logger.info(f"Loaded {len(df)} records from Excel")
            
            # Filter for care homes if requested
            if filter_care_homes is not None:
                if filter_care_homes:
                    df = df[df['Care home?'] == 'Y']
                    logger.info(f"Filtered to {len(df)} care home records")
                else:
                    df = df[df['Care home?'] != 'Y']
                    logger.info(f"Filtered to {len(df)} non-care home records")
            
            # Process each row
            for index, row in df.iterrows():
                try:
                    # Create provider first
                    provider = self.create_provider(row)
                    if not provider:
                        continue
                    
                    # Get or create location (static data)
                    location = self.get_or_create_location(row, provider)
                    if not location:
                        continue
                    
                    # Create time-varying period data
                    period_data = self.create_location_period_data(location, row, data_period)
                    if not period_data:
                        continue
                    
                    # Create activity flags for this period
                    activity_flags = self.create_location_activity_flags(location, row, data_period)
                    
                    # Add activities, service types, and user bands (for backward compatibility)
                    self.add_location_activities(location, row)
                    self.add_location_service_types(location, row)
                    self.add_location_user_bands(location, row)
                    
                    # Commit the relationships
                    self.db.commit()
                    
                    if (index + 1) % 100 == 0:
                        logger.info(f"Processed {index + 1} records")
                        
                except Exception as e:
                    self.db.rollback()
                    error_msg = f"Row {index}: {str(e)}"
                    self.stats["errors"].append(error_msg)
                    logger.error(error_msg)
                    continue
            
            # Process dual registrations if third sheet exists
            try:
                self.process_dual_registrations(excel_path, data_period)
            except Exception as e:
                logger.warning(f"Dual registration processing failed (sheet may not exist): {str(e)}")
                self.stats["errors"].append(f"Dual registration warning: {str(e)}")
            
            logger.info("Import completed successfully")
            return self.stats
            
        except Exception as e:
            logger.error(f"Import failed: {str(e)}")
            self.stats["errors"].append(f"Import failed: {str(e)}")
            return self.stats

    def process_dual_registrations(self, excel_path: str, data_period: DataPeriod):
        """Process dual registrations from third sheet"""
        try:
            # Try to load the third sheet (typically sheet index 2)
            excel_file = pd.ExcelFile(excel_path, engine='odf')
            sheet_names = excel_file.sheet_names
            
            logger.info(f"Found {len(sheet_names)} sheets: {sheet_names}")
            
            if len(sheet_names) < 3:
                logger.info("No third sheet found - skipping dual registration processing")
                return
            
            # Load the third sheet with minimal rows first to check structure
            third_sheet_name = sheet_names[2]
            logger.info(f"Processing dual registrations from sheet: '{third_sheet_name}'")
            
            # Load a sample first to understand structure
            try:
                df_sample = pd.read_excel(excel_path, engine='odf', sheet_name=third_sheet_name, nrows=3)
                logger.info(f"Third sheet columns: {list(df_sample.columns)}")
                logger.info(f"Sample shape: {df_sample.shape}")
                
                # Check if there's any data
                if df_sample.empty:
                    logger.info("Third sheet is empty - skipping dual registration processing")
                    return
                    
            except Exception as e:
                logger.warning(f"Could not read third sheet sample: {str(e)}")
                return
            
            # Now load the full third sheet
            df_dual = pd.read_excel(excel_path, engine='odf', sheet_name=third_sheet_name)
            logger.info(f"Loaded {len(df_dual)} rows from dual registration sheet")
            
            # Remove completely empty rows
            df_dual = df_dual.dropna(how='all')
            logger.info(f"After removing empty rows: {len(df_dual)} rows")
            
            if df_dual.empty:
                logger.info("No data found in third sheet after cleanup")
                return
            
            # Process each row in the dual registration sheet
            dual_pairs_processed = 0
            for index, row in df_dual.iterrows():
                try:
                    # Extract the specific dual registration identifiers
                    # Based on the actual column structure provided
                    location_id = self.clean_value(row.get('Location ID'))
                    linked_organisation_id = self.clean_value(row.get('Linked Organisation ID'))
                    primary_id = self.clean_value(row.get('Primary ID'))
                    
                    logger.debug(f"Row {index}: Location ID='{location_id}', Linked Org ID='{linked_organisation_id}', Primary ID='{primary_id}'")
                    
                    # We need both Location ID and Linked Organisation ID for dual registration
                    if not location_id or not linked_organisation_id:
                        logger.debug(f"Row {index}: Missing required IDs - Location ID: {location_id}, Linked Org ID: {linked_organisation_id}")
                        continue
                    
                    # Skip if they're the same (not a dual registration)
                    if location_id == linked_organisation_id:
                        logger.debug(f"Row {index}: Location ID and Linked Org ID are the same, skipping")
                        continue
                    
                    # Find both locations in the database
                    location1 = self.db.query(Location).join(LocationPeriodData).filter(
                        LocationPeriodData.period_id == data_period.period_id,
                        Location.location_id == location_id
                    ).first()
                    
                    location2 = self.db.query(Location).join(LocationPeriodData).filter(
                        LocationPeriodData.period_id == data_period.period_id,
                        Location.location_id == linked_organisation_id
                    ).first()
                    
                    if location1 and location2:
                        # Set dual registration links (bidirectional)
                        location1.dual_location_id = location2.location_id
                        location2.dual_location_id = location1.location_id
                        
                        # Set the dual registration flag
                        location1.is_dual_registered = True
                        location2.is_dual_registered = True
                        
                        # Also set primary_id if provided
                        if primary_id:
                            if primary_id == location_id:
                                location1.primary_id = location1.location_id
                                location2.primary_id = location1.location_id
                            elif primary_id == linked_organisation_id:
                                location1.primary_id = location2.location_id
                                location2.primary_id = location2.location_id
                        
                        self.db.commit()
                        dual_pairs_processed += 1
                        
                        relationship = self.clean_value(row.get('Relationship', 'dual registration'))
                        logger.info(f"✓ Linked dual registrations ({relationship}): '{location1.location_name}' ({location1.location_id}) <-> '{location2.location_name}' ({location2.location_id})")
                    else:
                        if not location1:
                            logger.debug(f"Could not find location with ID: {location_id}")
                        if not location2:
                            logger.debug(f"Could not find location with ID: {linked_organisation_id}")
                        
                except Exception as e:
                    logger.warning(f"Failed to process dual registration row {index}: {str(e)}")
                    continue
            
            logger.info(f"✓ Processed {dual_pairs_processed} dual registration pairs from {len(df_dual)} rows")
            self.stats["dual_registrations_processed"] = dual_pairs_processed
            
        except Exception as e:
            logger.error(f"Failed to process dual registrations: {str(e)}")
            # Don't raise the exception, just log it so the main import continues
            self.stats["dual_registrations_processed"] = 0