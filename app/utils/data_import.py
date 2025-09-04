import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pathlib import Path
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
# LocationActivityFlags import removed - table no longer used
from app.models.dual_registration import DualRegistration
from app.models.provider_brand import ProviderBrand

logger = logging.getLogger(__name__)


class CQCDataImporter:
    def __init__(self, db: Session):
        self.db = db
        self.stats = {
            "brands_created": 0,
            "providers_created": 0,
            "locations_created": 0,
            "location_period_data_created": 0,
            "activities_created": 0,
            "service_types_created": 0,
            "user_bands_created": 0,
            "periods_created": 0,
            "errors": []
        }

    def clean_value(self, value) -> Optional[str]:
        """Clean and validate string values - preserve asterisk (*) in text fields"""
        if pd.isna(value) or value == '-':
            return None
        if value == '*':
            return '*'  # Preserve asterisk for text fields
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
                # Convert to int if it's a float representing a whole number
                if isinstance(date_str, float) and date_str.is_integer():
                    date_str = int(date_str)
                
                # Excel stores dates as numbers since January 1, 1900
                # Handle common Excel date ranges (1900-2100) - serial dates 1 to 73050
                if 1 <= date_str <= 73050:
                    # Account for Excel leap year bug (1900 was not a leap year)
                    if date_str > 59:  # After Feb 28, 1900
                        excel_date = datetime(1899, 12, 30) + pd.Timedelta(days=date_str)
                    else:
                        excel_date = datetime(1899, 12, 31) + pd.Timedelta(days=date_str - 1)
                    return excel_date.date()
                else:
                    logger.warning(f"Excel date serial number out of reasonable range: {date_str}")
                    return None
            except Exception as e:
                logger.warning(f"Could not parse numeric date: {date_str} - {str(e)}")
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
        str_value = str(value).upper().strip()
        return str_value == 'Y' or str_value == 'DUAL REGISTRATION'

    def parse_number(self, value) -> Optional[int]:
        """Parse numeric values for integer fields - return None for asterisk (*)"""
        if pd.isna(value) or value == '' or value == '-' or value == '*':
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
        """Parse decimal values - return None for asterisk (*)"""
        if pd.isna(value) or value == '' or value == '*':
            return None
        try:
            return float(value)
        except:
            return None

    def parse_categorical_numeric(self, value) -> Optional[str]:
        """Parse categorical numeric data as strings, preserving leading zeros"""
        if pd.isna(value) or value == '' or value == '-':
            return None
        if value == '*':
            return '*'  # Preserve asterisk for categorical fields
        
        try:
            # Convert to string first to preserve format
            str_value = str(value).strip()
            
            # Handle scientific notation from Excel by converting to proper format
            if 'e+' in str_value.lower() or 'e-' in str_value.lower():
                # Convert scientific notation to full number string
                try:
                    float_val = float(str_value)
                    # If it's a whole number, format without decimals
                    if float_val == int(float_val):
                        str_value = str(int(float_val))
                    else:
                        str_value = str(float_val)
                except:
                    pass
            
            # For pure numeric strings that could have leading zeros, preserve them
            # Remove any trailing decimal zeros if present (e.g., "01.0" -> "01")
            if '.' in str_value and str_value.replace('.', '').isdigit():
                # Remove trailing zeros after decimal point
                str_value = str_value.rstrip('0').rstrip('.')
            
            return str_value if str_value else None
            
        except Exception as e:
            logger.warning(f"Could not parse categorical numeric: {value} - {str(e)}")
            return str(value).strip() if value else None

    def parse_primary_key(self, value, field_name: str = "") -> Optional[str]:
        """Parse primary key fields - return None for invalid values like * or -"""
        if pd.isna(value) or value == '' or value == '*' or value == '-':
            return None
            
        try:
            # Convert to string and clean
            str_value = str(value).strip()
            if not str_value or str_value in ['*', '-', 'nan', 'None']:
                return None
                
            # Handle scientific notation from Excel
            if 'e+' in str_value.lower() or 'e-' in str_value.lower():
                try:
                    float_val = float(str_value)
                    if float_val == int(float_val):
                        str_value = str(int(float_val))
                    else:
                        str_value = str(float_val)
                except:
                    pass
            
            # Remove trailing decimals if they're just .0
            if str_value.endswith('.0'):
                str_value = str_value[:-2]
                
            return str_value if str_value else None
            
        except Exception as e:
            logger.warning(f"Could not parse primary key {field_name}: {value} - {str(e)}")
            return None

    def parse_string_field(self, value, preserve_special: bool = True) -> Optional[str]:
        """Parse string fields - preserve * and - if preserve_special=True"""
        if pd.isna(value) or value == '':
            return None
            
        try:
            str_value = str(value).strip()
            if not str_value:
                return None
                
            # For string fields, we preserve special characters unless specified otherwise
            if not preserve_special and str_value in ['*', '-']:
                return None
                
            # Handle scientific notation from Excel for string fields that might contain numbers
            if 'e+' in str_value.lower() or 'e-' in str_value.lower():
                try:
                    float_val = float(str_value)
                    if float_val == int(float_val):
                        str_value = str(int(float_val))
                    else:
                        str_value = str(float_val)
                except:
                    pass  # Keep as string if conversion fails
            
            # Remove trailing .0 for string fields that look like numbers
            if str_value.endswith('.0') and str_value[:-2].replace('.', '').replace('-', '').isdigit():
                str_value = str_value[:-2]
                
            return str_value
            
        except Exception as e:
            logger.warning(f"Could not parse string field: {value} - {str(e)}")
            return str(value) if value else None

    def parse_numeric_field(self, value, allow_null_indicators: bool = True) -> Optional[int]:
        """Parse numeric fields - return None for * and - if allow_null_indicators=True"""
        if pd.isna(value) or value == '':
            return None
            
        if allow_null_indicators and str(value).strip() in ['*', '-']:
            return None
            
        try:
            # Handle scientific notation and large numbers
            if isinstance(value, str) and ('e+' in value.lower() or 'e-' in value.lower()):
                num = float(value)
            else:
                num = float(value)
                
            # Check for reasonable integer range
            if num > 9223372036854775807:  # Max BigInt value
                logger.warning(f"Numeric value too large: {value}")
                return None
                
            return int(num) if num == int(num) else None
        except Exception as e:
            logger.warning(f"Could not parse numeric field: {value} - {str(e)}")
            return None

    def parse_decimal_field(self, value, allow_null_indicators: bool = True) -> Optional[float]:
        """Parse decimal fields - return None for * and - if allow_null_indicators=True"""
        if pd.isna(value) or value == '':
            return None
            
        if allow_null_indicators and str(value).strip() in ['*', '-']:
            return None
            
        try:
            return float(value)
        except Exception as e:
            logger.warning(f"Could not parse decimal field: {value} - {str(e)}")
            return None

    def parse_boolean_field(self, value) -> Optional[bool]:
        """Parse boolean fields with comprehensive Y/N, True/False, 1/0 support"""
        if pd.isna(value) or value == '' or value == '*' or value == '-':
            return None
            
        try:
            str_val = str(value).strip().upper()
            
            # Handle Y/N format (most common in CQC data)
            if str_val in ['Y', 'YES', '1', 'TRUE', 'T']:
                return True
            elif str_val in ['N', 'NO', '0', 'FALSE', 'F']:
                return False
            else:
                logger.warning(f"Unrecognized boolean value: {value}")
                return None
                
        except Exception as e:
            logger.warning(f"Could not parse boolean field: {value} - {str(e)}")
            return None

    def parse_telephone(self, value) -> Optional[str]:
        """Parse telephone numbers as categorical strings, preserving leading zeros"""
        if pd.isna(value) or value == '' or value == '-':
            return None
        if value == '*':
            return '*'  # Preserve asterisk for phone fields
            
        try:
            # Use categorical numeric parsing to preserve leading zeros
            phone = self.parse_categorical_numeric(value)
            if not phone or phone == '*':
                return phone
            
            # Remove any non-digit characters for validation but keep original format
            import re
            digits_only = re.sub(r'[^\d]', '', phone)
            
            if not digits_only:
                return phone  # Return original if no digits found
                
            # For UK phone numbers, ensure proper format while preserving leading zeros
            if len(digits_only) == 11 and digits_only.startswith('0'):
                # Standard UK number - return with preserved format
                return phone
            elif len(digits_only) == 10 and not digits_only.startswith('0'):
                # Missing leading zero - add it while preserving other formatting
                if phone.isdigit():
                    return '0' + phone
                else:
                    return phone  # Keep original format if it has non-digit chars
            else:
                # Return original format for other cases
                return phone
                
        except Exception as e:
            logger.warning(f"Could not parse phone number: {value} - {str(e)}")
            return str(value).strip() if value else None

    def get_or_create_brand(self, brand_id: str, brand_name: str) -> Optional[Brand]:
        """Get existing brand by ID or create new one"""
        if not brand_id or brand_id == '-':
            return None
            
        brand = self.db.query(Brand).filter(Brand.brand_id == brand_id).first()
        if not brand:
            brand = Brand(
                brand_id=brand_id,
                brand_name=self.parse_string_field(brand_name, preserve_special=False) or f"Brand {brand_id}"
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



    def get_or_create_provider_by_original_id(self, row: pd.Series) -> Optional[Provider]:
        """Get existing provider by original ID or create new one with auto-increment ID"""
        provider_id = self.parse_primary_key(row.get('Provider ID'), 'Provider ID')
        if not provider_id:
            return None

        # Check if provider with this original ID already exists
        existing_provider = self.db.query(Provider).filter(
            Provider.provider_id == provider_id
        ).first()
        if existing_provider:
            return existing_provider


        provider = Provider(
            provider_id=provider_id,
            provider_name=self.parse_string_field(row.get('Provider Name'), preserve_special=False) or f"Provider {provider_id}",
            provider_hsca_start_date=self.parse_date(row.get('Provider HSCA start date')),
            provider_companies_house_number=self.parse_string_field(row.get('Provider Companies House Number'), preserve_special=True),
            provider_charity_number=self.parse_string_field(row.get('Provider Charity Number'), preserve_special=True),
            provider_type_sector=self.parse_string_field(row.get('Provider Type/Sector'), preserve_special=True),
            provider_inspection_directorate=self.parse_string_field(row.get('Provider Inspection Directorate'), preserve_special=True),
            provider_primary_inspection_category=self.clean_value(row.get('Provider Primary Inspection Category')),
            provider_ownership_type=self.parse_string_field(row.get('Provider Ownership Type'), preserve_special=True),
            provider_telephone_number=self.parse_telephone(row.get('Provider Telephone Number')),
            provider_web_address=self.parse_string_field(row.get('Provider Web Address'), preserve_special=True),
            provider_street_address=self.parse_string_field(row.get('Provider Street Address'), preserve_special=True),
            provider_address_line_2=self.parse_string_field(row.get('Provider Address Line 2'), preserve_special=True),
            provider_city=self.parse_string_field(row.get('Provider City'), preserve_special=True),
            provider_county=self.parse_string_field(row.get('Provider County'), preserve_special=True),
            provider_postal_code=self.parse_categorical_numeric(row.get('Provider Postal Code')),
            provider_paf_id=self.parse_categorical_numeric(row.get('Provider PAF ID')),
            provider_uprn_id=self.parse_categorical_numeric(row.get('Provider UPRN ID')),
            provider_local_authority=self.parse_string_field(row.get('Provider Local Authority'), preserve_special=True),
            provider_region=self.parse_string_field(row.get('Provider Region'), preserve_special=True),
            provider_nhs_region=self.parse_string_field(row.get('Provider NHS Region'), preserve_special=True),
            provider_latitude=self.parse_decimal_field(row.get('Provider Latitude')),
            provider_longitude=self.parse_decimal_field(row.get('Provider Longitude')),
            provider_parliamentary_constituency=self.parse_string_field(row.get('Provider Parliamentary Constituency'), preserve_special=True),
            provider_nominated_individual_name=self.parse_string_field(row.get('Provider Nominated Individual Name'), preserve_special=True),
            provider_main_partner_name=self.parse_string_field(row.get('Provider Main Partner Name'), preserve_special=True)
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

    def get_or_create_location_by_original_id(self, row: pd.Series, provider: Provider) -> Optional[Location]:
        """Get existing location by original ID or create new one (static data only)"""
        location_id = self.parse_primary_key(row.get('Location ID'), 'Location ID')
        if not location_id:
            return None

        # Check if location already exists
        existing_location = self.db.query(Location).filter(
            Location.location_id == location_id
        ).first()
        if existing_location:
            return existing_location

        # Create new location with static data only
        location = Location(
            location_id=location_id,
            provider_id=provider.provider_id,
            location_name=self.parse_string_field(row.get('Location Name'), preserve_special=False) or f"Location {location_id}",
            location_hsca_start_date=self.parse_date(row.get('Location HSCA start date')),
            location_ods_code=self.parse_categorical_numeric(row.get('Location ODS Code')),
            location_telephone_number=self.parse_telephone(row.get('Location Telephone Number')),
            location_web_address=self.parse_string_field(row.get('Location Web Address'), preserve_special=True),
            location_type_sector=self.parse_string_field(row.get('Location Type/Sector'), preserve_special=True),
            location_inspection_directorate=self.parse_string_field(row.get('Location Inspection Directorate'), preserve_special=True),
            location_primary_inspection_category=self.parse_string_field(row.get('Location Primary Inspection Category'), preserve_special=True),
            location_region=self.parse_string_field(row.get('Location Region'), preserve_special=True),
            location_nhs_region=self.parse_string_field(row.get('Location NHS Region'), preserve_special=True),
            location_local_authority=self.parse_string_field(row.get('Location Local Authority'), preserve_special=True),
            location_onspd_ccg_code=self.parse_categorical_numeric(row.get('Location ONSPD CCG Code')),
            location_onspd_ccg=self.clean_value(row.get('Location ONSPD CCG')),
            location_commissioning_ccg_code=self.parse_categorical_numeric(row.get('Location Commissioning CCG Code')),
            location_commissioning_ccg=self.clean_value(row.get('Location Commissioning CCG')),
            location_street_address=self.clean_value(row.get('Location Street Address')),
            location_address_line_2=self.parse_string_field(row.get('Location Address Line 2'), preserve_special=True),
            location_city=self.parse_string_field(row.get('Location City'), preserve_special=True),
            location_county=self.parse_string_field(row.get('Location County'), preserve_special=True),
            location_postal_code=self.parse_categorical_numeric(row.get('Location Postal Code')),
            location_paf_id=self.parse_categorical_numeric(row.get('Location PAF ID')),
            location_uprn_id=self.parse_categorical_numeric(row.get('Location UPRN ID')),
            location_latitude=self.parse_decimal_field(row.get('Location Latitude')),
            location_longitude=self.parse_decimal_field(row.get('Location Longitude')),
            location_parliamentary_constituency=self.parse_string_field(row.get('Location Parliamentary Constituency'), preserve_special=True),
            location_also_known_as=self.parse_string_field(row.get('Location Also Known As'), preserve_special=True),
            location_specialisms=self.parse_string_field(row.get('Location Specialisms'), preserve_special=True)
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
            is_dormant=self.parse_boolean_field(row.get('Dormant (Y/N)')),
            is_care_home=self.parse_boolean_field(row.get('Care home?')),
            registered_manager=self.parse_string_field(row.get('Registered manager'), preserve_special=True),
            care_homes_beds=self.parse_numeric_field(row.get('Care homes beds')),
            latest_overall_rating=self.parse_string_field(row.get('Location Latest Overall Rating'), preserve_special=True),
            publication_date=self.validate_date(self.parse_date(row.get('Publication Date')), 'publication_date'),
            is_inherited_rating=self.parse_boolean_field(row.get('Inherited Rating (Y/N)'))
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


    # create_location_activity_flags function REMOVED - no longer needed
    # Boolean data now stored in normalized association tables:
    # - location_regulated_activities 
    # - location_service_types
    # - location_service_user_bands

    def create_provider_brand_relationship(self, provider: Provider, brand: Brand, data_period: DataPeriod) -> Optional[ProviderBrand]:
        """Create provider-brand relationship for a specific period"""
        if not brand:
            # No brand for this provider in this period
            return None
            
        # Check if relationship already exists for this period
        existing_relationship = self.db.query(ProviderBrand).filter(
            ProviderBrand.provider_id == provider.provider_id,
            ProviderBrand.brand_id == brand.brand_id,
            ProviderBrand.period_id == data_period.period_id
        ).first()
        
        if existing_relationship:
            return existing_relationship
        
        # Create new provider-brand relationship
        provider_brand = ProviderBrand(
            provider_id=provider.provider_id,
            brand_id=brand.brand_id,
            period_id=data_period.period_id
        )
        
        try:
            self.db.add(provider_brand)
            self.db.commit()
            logger.info(f"Created provider-brand relationship: {provider.provider_name} -> {brand.brand_name} for period {data_period.year}-{data_period.month}")
            return provider_brand
        except IntegrityError as e:
            self.db.rollback()
            logger.warning(f"Provider-brand relationship {provider.provider_id}-{brand.brand_id}-{data_period.period_id}: {str(e)}")
            return self.db.query(ProviderBrand).filter(
                ProviderBrand.provider_id == provider.provider_id,
                ProviderBrand.brand_id == brand.brand_id,
                ProviderBrand.period_id == data_period.period_id
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
            
            # Scan headers and populate lookup tables dynamically
            lookup_mappings = self.scan_and_populate_lookup_tables(df)
            
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
                    provider = self.get_or_create_provider_by_original_id(row)
                    if not provider:
                        continue
                    
                    # Create provider-brand relationship for this period
                    brand_id = self.parse_primary_key(row.get('Brand ID'), 'Brand ID')
                    brand_name = self.parse_string_field(row.get('Brand Name'), preserve_special=False)
                    brand = None
                    if brand_id and brand_id != '-':
                        brand = self.get_or_create_brand(brand_id, brand_name)
                    self.create_provider_brand_relationship(provider, brand, data_period)
                    
                    # Get or create location (static data)
                    location = self.get_or_create_location_by_original_id(row, provider)
                    if not location:
                        continue
                    
                    # Create time-varying period data
                    period_data = self.create_location_period_data(location, row, data_period)
                    if not period_data:
                        continue
                    
                    # Note: LocationActivityFlags table no longer used - data now in association tables
                    
                    # Create dynamic associations based on discovered columns
                    self.create_dynamic_associations(location, row, data_period, lookup_mappings)
                    
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
            
            # Process dual registrations from third sheet if it exists (independent of main sheet columns)
            logger.info("Processing dual registrations from third sheet (if available)...")
            try:
                self.process_dual_registrations(excel_path, data_period)
            except Exception as e:
                logger.info(f"Dual registration sheet not available or empty: {str(e)}")
                # This is not an error - dual registration data is optional
                logger.info("Continuing without dual registration data (this is normal if no dual registrations exist)")
            
            logger.info("Import completed successfully")
            return self.stats
            
        except Exception as e:
            logger.error(f"Import failed: {str(e)}")
            self.stats["errors"].append(f"Import failed: {str(e)}")
            return self.stats

    def process_dual_registrations(self, excel_path: str, data_period: DataPeriod):
        """Process dual registrations from third sheet if available"""
        try:
            # Try to load the Excel file and check sheet structure
            try:
                excel_file = pd.ExcelFile(excel_path, engine='odf')
                sheet_names = excel_file.sheet_names
                logger.info(f"Found {len(sheet_names)} sheets: {sheet_names}")
            except Exception as e:
                logger.info(f"Could not read Excel file structure: {str(e)}")
                return
            
            # Check if there are at least 3 sheets
            if len(sheet_names) < 3:
                logger.info("No third sheet found - dual registration data not available")
                return
            
            # Try to access the third sheet
            third_sheet_name = sheet_names[2]
            logger.info(f"Attempting to process dual registrations from sheet: '{third_sheet_name}'")
            
            # Load a sample first to check if sheet has data and expected structure
            try:
                df_sample = pd.read_excel(excel_path, engine='odf', sheet_name=third_sheet_name, nrows=5)
                
                # Check if there's any data at all
                if df_sample.empty:
                    logger.info("Third sheet is empty - no dual registration data to process")
                    return
                    
                # Check if it has the expected columns for dual registration
                expected_columns = ['Location ID', 'Linked Organisation ID']
                missing_columns = [col for col in expected_columns if col not in df_sample.columns]
                
                if missing_columns:
                    logger.info(f"Third sheet doesn't appear to contain dual registration data (missing columns: {missing_columns})")
                    logger.info(f"Available columns: {list(df_sample.columns)}")
                    return
                    
                logger.info(f"Third sheet appears to contain dual registration data with columns: {list(df_sample.columns)}")
                
            except Exception as e:
                logger.info(f"Could not read third sheet sample: {str(e)} - assuming no dual registration data")
                return
            
            # Now try to load the full third sheet
            try:
                df_dual = pd.read_excel(excel_path, engine='odf', sheet_name=third_sheet_name)
                logger.info(f"Loaded {len(df_dual)} rows from dual registration sheet")
                
                # Remove completely empty rows
                df_dual = df_dual.dropna(how='all')
                logger.info(f"After removing empty rows: {len(df_dual)} rows")
                
                if df_dual.empty:
                    logger.info("No dual registration data found after cleanup")
                    return
            except Exception as e:
                logger.info(f"Could not load full dual registration sheet: {str(e)}")
                return
            
            # Process each row in the dual registration sheet
            dual_pairs_processed = 0
            for index, row in df_dual.iterrows():
                try:
                    # Extract dual registration data from the sheet
                    location_id = self.parse_primary_key(row.get('Location ID'), 'Location ID')
                    linked_organisation_id = self.clean_value(row.get('Linked Organisation ID'))
                    relationship_type = self.clean_value(row.get('Relationship'))
                    relationship_start_date = self.parse_date(row.get('Relationship Start Date'))
                    primary_id = self.clean_value(row.get('Primary ID'))
                    
                    logger.debug(f"Row {index}: Location ID='{location_id}', Linked Org ID='{linked_organisation_id}', Relationship='{relationship_type}'")
                    
                    # We need both Location ID and Linked Organisation ID for dual registration
                    if not location_id or not linked_organisation_id:
                        logger.debug(f"Row {index}: Missing required IDs - Location ID: {location_id}, Linked Org ID: {linked_organisation_id}")
                        continue
                    
                    # Skip if they're the same (not a dual registration)
                    if location_id == linked_organisation_id:
                        logger.debug(f"Row {index}: Location ID and Linked Org ID are the same, skipping")
                        continue
                    
                    # Verify both locations exist
                    location1 = self.db.query(Location).filter(Location.location_id == location_id).first()
                    location2 = self.db.query(Location).filter(Location.location_id == linked_organisation_id).first()
                    
                    if location1 and location2:
                        # Determine which is primary (Primary ID field indicates if the current location is primary)
                        is_location_primary = self.parse_boolean(primary_id)
                        # For the linked location, the primary status is the opposite
                        is_linked_primary = not is_location_primary
                        
                        # Create dual registration records (bidirectional)
                        dual_reg_1 = DualRegistration(
                            location_id=location1.location_id,
                            linked_organisation_id=location2.location_id,
                            period_id=data_period.period_id,
                            relationship_type=relationship_type,
                            relationship_start_date=relationship_start_date,
                            is_primary=is_location_primary
                        )
                        
                        dual_reg_2 = DualRegistration(
                            location_id=location2.location_id,
                            linked_organisation_id=location1.location_id,
                            period_id=data_period.period_id,
                            relationship_type=relationship_type,
                            relationship_start_date=relationship_start_date,
                            is_primary=is_linked_primary
                        )
                        
                        # Check if these dual registrations already exist for this period
                        existing_1 = self.db.query(DualRegistration).filter(
                            DualRegistration.location_id == location1.location_id,
                            DualRegistration.linked_organisation_id == location2.location_id,
                            DualRegistration.period_id == data_period.period_id
                        ).first()
                        
                        existing_2 = self.db.query(DualRegistration).filter(
                            DualRegistration.location_id == location2.location_id,
                            DualRegistration.linked_organisation_id == location1.location_id,
                            DualRegistration.period_id == data_period.period_id
                        ).first()
                        
                        if not existing_1:
                            self.db.add(dual_reg_1)
                        if not existing_2:
                            self.db.add(dual_reg_2)
                            
                        self.db.commit()
                        dual_pairs_processed += 1
                        
                        logger.info(f"✓ Created dual registrations ({relationship_type}): '{location1.location_name}' ({location1.location_id}) <-> '{location2.location_name}' ({location2.location_id}) for {data_period.year}-{data_period.month:02d}")
                    else:
                        if not location1:
                            logger.debug(f"Could not find location with original ID: {location_id}")
                        if not location2:
                            logger.debug(f"Could not find location with original ID: {linked_organisation_id}")
                        
                except Exception as e:
                    logger.warning(f"Failed to process dual registration row {index}: {str(e)}")
                    continue
            
            logger.info(f"✓ Processed {dual_pairs_processed} dual registration pairs from {len(df_dual)} rows")
            self.stats["dual_registrations_processed"] = dual_pairs_processed
            
        except Exception as e:
            logger.info(f"Dual registration processing completed with info: {str(e)}")
            # This is informational - dual registrations are optional
            self.stats["dual_registrations_processed"] = 0

    def import_from_parquet(self, main_parquet_path: str, dual_parquet_path: str, filter_care_homes: bool = None, year: int = None, month: int = None) -> Dict:
        """
        Optimized import from Parquet files with dual registration lookup
        
        Args:
            main_parquet_path: Path to main data Parquet file
            dual_parquet_path: Path to dual registration Parquet file
            filter_care_homes: Filter for care homes
            year: Data year
            month: Data month
            
        Returns:
            Import statistics
        """
        try:
            import time
            start_time = time.time()
            
            logger.info(f"🚀 Starting optimized Parquet import: {main_parquet_path}")
            logger.info(f"📊 Target period: {year}-{month:02d}")
            
            # Validate year and month parameters
            if year is None or month is None:
                raise ValueError("Both year and month parameters are required")
            
            if not (1 <= month <= 12):
                raise ValueError("Month must be between 1 and 12")
                
            # Create or get data period
            file_name = Path(main_parquet_path).name
            data_period = self.get_or_create_data_period(year, month, file_name)
            logger.info(f"✅ Data period established: {year}-{month:02d} (ID: {data_period.period_id})")
            
            # Load main data Parquet file
            logger.info("📖 Step 1: Loading main data from Parquet file...")
            main_file_size = Path(main_parquet_path).stat().st_size / (1024 * 1024)
            logger.info(f"📁 Main Parquet file size: {main_file_size:.1f} MB")
            
            df_main = pd.read_parquet(main_parquet_path)
            logger.info(f"✅ Loaded {len(df_main)} records from main Parquet file")
            logger.info(f"📋 Columns available: {len(df_main.columns)} columns")
            
            # Scan headers and populate lookup tables dynamically
            lookup_mappings = self.scan_and_populate_lookup_tables(df_main)
            
            # Load dual registration Parquet file and create lookup
            logger.info("🔗 Step 2: Loading dual registration data from Parquet file...")
            dual_file_size = Path(dual_parquet_path).stat().st_size / 1024
            logger.info(f"📁 Dual registration Parquet file size: {dual_file_size:.1f} KB")
            
            df_dual = pd.read_parquet(dual_parquet_path)
            logger.info(f"📋 Dual registration records: {len(df_dual)} rows")
            
            # Create efficient dual registration lookup dictionary
            logger.info("🔍 Step 3: Building dual registration lookup dictionary...")
            dual_lookup = {}
            if not df_dual.empty:
                # Create lookup by Location ID for fast access
                for index, dual_row in df_dual.iterrows():
                    location_id = self.clean_value(dual_row.get('Location ID'))
                    if location_id:
                        dual_lookup[location_id] = {
                            'linked_organisation_id': self.clean_value(dual_row.get('Linked Organisation ID')),
                            'relationship_type': self.clean_value(dual_row.get('Relationship')),
                            'relationship_start_date': self.parse_date(dual_row.get('Relationship Start Date')),
                            'primary_id': self.clean_value(dual_row.get('Primary ID'))
                        }
                        
                    if (index + 1) % 50 == 0 or index + 1 == len(df_dual):
                        logger.info(f"   🔄 Processed {index + 1}/{len(df_dual)} dual registration records")
            
            logger.info(f"✅ Dual registration lookup created: {len(dual_lookup)} location mappings")
            
            # Filter for care homes if requested
            logger.info("🔽 Step 4: Applying data filters...")
            if filter_care_homes is not None:
                original_count = len(df_main)
                if filter_care_homes:
                    df_main = df_main[df_main['Care home?'] == 'Y']
                    logger.info(f"✅ Care homes filter applied: {len(df_main)} records (from {original_count})")
                else:
                    df_main = df_main[df_main['Care home?'] != 'Y']
                    logger.info(f"✅ Non-care homes filter applied: {len(df_main)} records (from {original_count})")
            else:
                logger.info(f"✅ No filter applied: processing all {len(df_main)} records")
            
            # Process main data
            logger.info("🔄 Step 5: Processing main data records...")
            processed_count = 0
            providers_created = 0
            locations_created = 0
            
            for index, row in df_main.iterrows():
                try:
                    current_record = index + 1
                    
                    # Progress logging every 100 records and at specific milestones
                    if current_record % 100 == 0 or current_record in [1, 10, 50] or current_record == len(df_main):
                        progress_pct = (current_record / len(df_main)) * 100
                        logger.info(f"   📝 Processing record {current_record}/{len(df_main)} ({progress_pct:.1f}%)")
                    
                    # Create provider first
                    provider_id = self.parse_primary_key(row.get('Provider ID'), 'Provider ID')
                    if current_record <= 10:  # Log details for first 10 records
                        logger.info(f"      🏢 Processing provider: {provider_id}")
                    
                    provider = self.get_or_create_provider_by_original_id(row)
                    if not provider:
                        if current_record <= 10:
                            logger.warning(f"      ⚠️  Skipped record {current_record}: no provider created")
                        continue
                    
                    # Create provider-brand relationship for this period
                    brand_id = self.parse_primary_key(row.get('Brand ID'), 'Brand ID')
                    brand_name = self.parse_string_field(row.get('Brand Name'), preserve_special=False)
                    brand = None
                    if brand_id and brand_id != '-':
                        brand = self.get_or_create_brand(brand_id, brand_name)
                    self.create_provider_brand_relationship(provider, brand, data_period)
                    
                    # Track if this is a new provider
                    if provider_id not in getattr(self, '_seen_providers', set()):
                        providers_created += 1
                        if not hasattr(self, '_seen_providers'):
                            self._seen_providers = set()
                        self._seen_providers.add(provider_id)
                    
                    # Get or create location (static data)
                    location_id = self.parse_primary_key(row.get('Location ID'), 'Location ID')
                    location_name = self.parse_string_field(row.get('Location Name'), preserve_special=False)
                    
                    if current_record <= 10:
                        logger.info(f"      🏠 Processing location: {location_id} - {location_name}")
                    
                    location = self.get_or_create_location_by_original_id(row, provider)
                    if not location:
                        if current_record <= 10:
                            logger.warning(f"      ⚠️  Skipped record {current_record}: no location created")
                        continue
                    
                    # Track if this is a new location
                    if location_id not in getattr(self, '_seen_locations', set()):
                        locations_created += 1
                        if not hasattr(self, '_seen_locations'):
                            self._seen_locations = set()
                        self._seen_locations.add(location_id)
                    
                    # Create time-varying period data
                    period_data = self.create_location_period_data(location, row, data_period)
                    if not period_data:
                        if current_record <= 10:
                            logger.warning(f"      ⚠️  Skipped record {current_record}: no period data created")
                        continue
                    
                    # Note: LocationActivityFlags table no longer used - data now in association tables
                    
                    # Create dynamic associations based on discovered columns
                    self.create_dynamic_associations(location, row, data_period, lookup_mappings)
                    
                    # Note: Dual registration processing moved to separate step after main data processing
                    
                    # Commit the record
                    self.db.commit()
                    processed_count += 1
                    
                    # Progress updates at key intervals
                    if processed_count % 500 == 0:
                        elapsed = time.time() - start_time
                        rate = processed_count / elapsed
                        eta = (len(df_main) - processed_count) / rate if rate > 0 else 0
                        logger.info(f"   ⏱️  Progress: {processed_count}/{len(df_main)} records ({rate:.1f} rec/sec, ETA: {eta/60:.1f}min)")
                        
                except Exception as e:
                    self.db.rollback()
                    error_msg = f"❌ Row {current_record}: {str(e)}"
                    self.stats["errors"].append(error_msg)
                    logger.error(error_msg)
                    continue
            
            # Process dual registrations separately after main data
            logger.info("🔗 Step 6: Processing dual registrations...")
            dual_registrations_created = 0
            
            if not df_dual.empty:
                logger.info(f"📋 Processing {len(dual_lookup)} dual registration mappings...")
                
                for location_id, dual_info in dual_lookup.items():
                    try:
                        linked_organisation_id = dual_info['linked_organisation_id']
                        relationship_type = dual_info['relationship_type']
                        
                        if linked_organisation_id and linked_organisation_id != location_id:
                            # Verify both locations exist
                            location = self.db.query(Location).filter(Location.location_id == location_id).first()
                            linked_location = self.db.query(Location).filter(Location.location_id == linked_organisation_id).first()
                            
                            if location and linked_location:
                                # Create dual registration records (bidirectional)
                                primary_id_value = dual_info['primary_id']
                                is_location_primary = self.parse_boolean(primary_id_value)
                                is_linked_primary = not is_location_primary
                                
                                # Create dual registration record for current location
                                existing_dual = self.db.query(DualRegistration).filter(
                                    DualRegistration.location_id == location.location_id,
                                    DualRegistration.linked_organisation_id == linked_location.location_id,
                                    DualRegistration.period_id == data_period.period_id
                                ).first()
                                
                                if not existing_dual:
                                    dual_reg = DualRegistration(
                                        location_id=location.location_id,
                                        linked_organisation_id=linked_location.location_id,
                                        period_id=data_period.period_id,
                                        relationship_type=relationship_type,
                                        relationship_start_date=dual_info['relationship_start_date'],
                                        is_primary=is_location_primary
                                    )
                                    self.db.add(dual_reg)
                                    dual_registrations_created += 1
                                
                                # Create reverse dual registration record
                                existing_dual_reverse = self.db.query(DualRegistration).filter(
                                    DualRegistration.location_id == linked_location.location_id,
                                    DualRegistration.linked_organisation_id == location.location_id,
                                    DualRegistration.period_id == data_period.period_id
                                ).first()
                                
                                if not existing_dual_reverse:
                                    dual_reg_reverse = DualRegistration(
                                        location_id=linked_location.location_id,
                                        linked_organisation_id=location.location_id,
                                        period_id=data_period.period_id,
                                        relationship_type=relationship_type,
                                        relationship_start_date=dual_info['relationship_start_date'],
                                        is_primary=is_linked_primary
                                    )
                                    self.db.add(dual_reg_reverse)
                                
                                # Commit dual registration records
                                self.db.commit()
                                
                                logger.info(f"✅ Created dual registration: {location.location_name} ↔ {linked_location.location_name}")
                            else:
                                if not location:
                                    logger.warning(f"⚠️  Location not found with original ID: {location_id}")
                                if not linked_location:
                                    logger.warning(f"⚠️  Linked location not found with original ID: {linked_organisation_id}")
                    except Exception as e:
                        logger.warning(f"❌ Failed to process dual registration for {location_id}: {str(e)}")
                        continue
                
                logger.info(f"✅ Dual registration processing complete: {dual_registrations_created} pairs created")
            else:
                logger.info("📋 No dual registration data found")

            # Calculate final statistics
            total_time = time.time() - start_time
            records_per_second = processed_count / total_time if total_time > 0 else 0
            
            # Update statistics
            self.stats["dual_registrations_processed"] = dual_registrations_created
            self.stats["import_time_seconds"] = total_time
            self.stats["records_per_second"] = records_per_second
            self.stats["records_processed"] = processed_count
            self.stats["providers_processed"] = providers_created
            self.stats["locations_processed"] = locations_created
            
            # Final summary
            logger.info("🎉 PARQUET IMPORT SUMMARY:")
            logger.info(f"   📊 Records processed: {processed_count}/{len(df_main)}")
            logger.info(f"   🏢 Providers processed: {providers_created}")
            logger.info(f"   🏠 Locations processed: {locations_created}")
            logger.info(f"   🔗 Dual registrations created: {dual_registrations_created}")
            logger.info(f"   ⏱️  Total import time: {total_time:.2f} seconds")
            logger.info(f"   🚀 Processing speed: {records_per_second:.1f} records/second")
            logger.info(f"   📈 Performance: {len(df_main) / (total_time/60):.0f} records/minute")
            
            if self.stats["errors"]:
                logger.warning(f"   ⚠️  Errors encountered: {len(self.stats['errors'])}")
            
            logger.info("✅ Optimized Parquet import completed successfully!")
            
            return self.stats
            
        except Exception as e:
            logger.error(f"Parquet import failed: {str(e)}")
            self.stats["errors"].append(f"Parquet import failed: {str(e)}")
            return self.stats

    def scan_and_populate_lookup_tables(self, df: pd.DataFrame) -> Dict[str, Dict[str, int]]:
        """
        Scan CSV/Excel headers and populate lookup tables dynamically
        Returns mapping of column names to database IDs for each category
        """
        logger.info("🔍 Scanning headers and populating lookup tables...")
        
        lookup_mappings = {
            'regulated_activities': {},
            'service_types': {},
            'service_user_bands': {}
        }
        
        for column in df.columns:
            # Check for regulated activity columns
            if column.startswith('Regulated activity - '):
                full_name = column  # Store the complete column name
                activity_id = self.get_or_create_regulated_activity_dynamic(full_name)
                lookup_mappings['regulated_activities'][column] = activity_id
                
            # Check for service type columns  
            elif column.startswith('Service type - '):
                full_name = column  # Store the complete column name
                service_type_id = self.get_or_create_service_type_dynamic(full_name)
                lookup_mappings['service_types'][column] = service_type_id
                
            # Check for service user band columns
            elif column.startswith('Service user band - '):
                full_name = column  # Store the complete column name
                band_id = self.get_or_create_service_user_band_dynamic(full_name)
                lookup_mappings['service_user_bands'][column] = band_id
        
        logger.info(f"✅ Lookup tables populated:")
        logger.info(f"   - {len(lookup_mappings['regulated_activities'])} regulated activities")
        logger.info(f"   - {len(lookup_mappings['service_types'])} service types")
        logger.info(f"   - {len(lookup_mappings['service_user_bands'])} service user bands")
        
        return lookup_mappings

    def get_or_create_regulated_activity_dynamic(self, full_column_name: str) -> int:
        """Get or create a regulated activity with the full column name"""
        # Check if it already exists
        existing = self.db.query(RegulatedActivity).filter(
            RegulatedActivity.activity_name == full_column_name
        ).first()
        
        if existing:
            return existing.activity_id
            
        # Create new activity
        new_activity = RegulatedActivity(activity_name=full_column_name)
        self.db.add(new_activity)
        self.db.commit()
        
        logger.info(f"📝 Created regulated activity: {full_column_name}")
        return new_activity.activity_id

    def get_or_create_service_type_dynamic(self, full_column_name: str) -> int:
        """Get or create a service type with the full column name"""
        # Check if it already exists
        existing = self.db.query(ServiceType).filter(
            ServiceType.service_type_name == full_column_name
        ).first()
        
        if existing:
            return existing.service_type_id
            
        # Create new service type
        new_service_type = ServiceType(service_type_name=full_column_name)
        self.db.add(new_service_type)
        self.db.commit()
        
        logger.info(f"📝 Created service type: {full_column_name}")
        return new_service_type.service_type_id

    def get_or_create_service_user_band_dynamic(self, full_column_name: str) -> int:
        """Get or create a service user band with the full column name"""
        # Check if it already exists
        existing = self.db.query(ServiceUserBand).filter(
            ServiceUserBand.band_name == full_column_name
        ).first()
        
        if existing:
            return existing.band_id
            
        # Create new service user band
        new_band = ServiceUserBand(band_name=full_column_name)
        self.db.add(new_band)
        self.db.commit()
        
        logger.info(f"📝 Created service user band: {full_column_name}")
        return new_band.band_id

    def create_dynamic_associations(self, location: Location, row: pd.Series, data_period: DataPeriod, lookup_mappings: Dict[str, Dict[str, int]]):
        """Create association records only when boolean values are True using dynamic lookups"""
        
        # Process regulated activities
        for column, activity_id in lookup_mappings['regulated_activities'].items():
            if self.parse_boolean_field(row.get(column)):
                # Check if association already exists
                existing = self.db.query(LocationRegulatedActivity).filter(
                    LocationRegulatedActivity.location_id == location.location_id,
                    LocationRegulatedActivity.activity_id == activity_id,
                    LocationRegulatedActivity.period_id == data_period.period_id
                ).first()
                
                if not existing:
                    association = LocationRegulatedActivity(
                        location_id=location.location_id,
                        activity_id=activity_id,
                        period_id=data_period.period_id
                    )
                    self.db.add(association)
                    self.stats["activities_created"] += 1
        
        # Process service types
        for column, service_type_id in lookup_mappings['service_types'].items():
            if self.parse_boolean_field(row.get(column)):
                # Check if association already exists
                existing = self.db.query(LocationServiceType).filter(
                    LocationServiceType.location_id == location.location_id,
                    LocationServiceType.service_type_id == service_type_id,
                    LocationServiceType.period_id == data_period.period_id
                ).first()
                
                if not existing:
                    association = LocationServiceType(
                        location_id=location.location_id,
                        service_type_id=service_type_id,
                        period_id=data_period.period_id
                    )
                    self.db.add(association)
                    self.stats["service_types_created"] += 1
        
        # Process service user bands
        for column, band_id in lookup_mappings['service_user_bands'].items():
            if self.parse_boolean_field(row.get(column)):
                # Check if association already exists
                existing = self.db.query(LocationServiceUserBand).filter(
                    LocationServiceUserBand.location_id == location.location_id,
                    LocationServiceUserBand.band_id == band_id,
                    LocationServiceUserBand.period_id == data_period.period_id
                ).first()
                
                if not existing:
                    association = LocationServiceUserBand(
                        location_id=location.location_id,
                        band_id=band_id,
                        period_id=data_period.period_id
                    )
                    self.db.add(association)
                    self.stats["user_bands_created"] += 1