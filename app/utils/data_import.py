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
        """Clean and validate string values"""
        if pd.isna(value) or value == '-' or value == '*':
            return None
        result = str(value).strip() if value else None
        # Truncate very long strings to prevent database errors
        if result and len(result) > 250:
            result = result[:250]
        return result

    def parse_date(self, date_str) -> Optional[datetime]:
        """Parse date string to datetime object"""
        if pd.isna(date_str) or not date_str:
            return None
        
        # Skip numeric values that aren't dates
        if isinstance(date_str, (int, float)):
            return None
            
        try:
            if isinstance(date_str, str):
                # Try different date formats
                for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"]:
                    try:
                        return datetime.strptime(date_str, fmt).date()
                    except:
                        continue
                return None
            elif hasattr(date_str, 'date'):
                return date_str.date()
            elif hasattr(date_str, 'to_pydatetime'):
                return date_str.to_pydatetime().date()
            return date_str
        except:
            return None

    def parse_boolean(self, value) -> bool:
        """Parse Y/N values to boolean"""
        if pd.isna(value):
            return False
        return str(value).upper() == 'Y'

    def parse_number(self, value) -> Optional[int]:
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
            telephone_number=self.parse_number(row.get('Provider Telephone Number')),
            web_address=self.clean_value(row.get('Provider Web Address')),
            street_address=self.clean_value(row.get('Provider Street Address')),
            address_line_2=self.clean_value(row.get('Provider Address Line 2')),
            city=self.clean_value(row.get('Provider City')),
            county=self.clean_value(row.get('Provider County')),
            postal_code=self.clean_value(row.get('Provider Postal Code')),
            paf_id=self.parse_number(row.get('Provider PAF ID')),
            uprn_id=self.parse_number(row.get('Provider UPRN ID')),
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
            location_telephone_number=self.parse_number(row.get('Location Telephone Number')),
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
            location_paf_id=self.parse_number(row.get('Location PAF ID')),
            location_uprn_id=self.parse_number(row.get('Location UPRN ID')),
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
            publication_date=self.parse_date(row.get('Publication Date')),
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
                    
                    # Add activities, service types, and user bands
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
            
            logger.info("Import completed successfully")
            return self.stats
            
        except Exception as e:
            logger.error(f"Import failed: {str(e)}")
            self.stats["errors"].append(f"Import failed: {str(e)}")
            return self.stats