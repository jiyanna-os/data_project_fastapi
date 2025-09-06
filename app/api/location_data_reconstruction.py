from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Dict, Any, List, Optional
import logging
from app.core.database import get_db
from app.models.regulated_activity import RegulatedActivity
from app.models.service_type import ServiceType  
from app.models.service_user_band import ServiceUserBand

router = APIRouter()
logger = logging.getLogger(__name__)


def reconstruct_boolean_columns(db: Session, location_id: str, period_id: int = None) -> Dict[str, str]:
    """
    Dynamically reconstruct all boolean columns from association tables.
    Returns a dictionary with original column names and Y/N values.
    """
    boolean_columns = {}
    
    # Get all regulated activities and check which ones this location has
    activities = db.query(RegulatedActivity).all()
    for activity in activities:
        # Query if this location has this activity for the period
        if period_id:
            has_activity = db.execute(
                text("SELECT 1 FROM location_regulated_activities WHERE location_id = :location_id AND activity_id = :activity_id AND period_id = :period_id"),
                {"location_id": location_id, "activity_id": activity.activity_id, "period_id": period_id}
            ).fetchone()
        else:
            has_activity = db.execute(
                text("SELECT 1 FROM location_regulated_activities WHERE location_id = :location_id AND activity_id = :activity_id"),
                {"location_id": location_id, "activity_id": activity.activity_id}
            ).fetchone()
        
        # Store as original column name with Y/N value
        boolean_columns[activity.activity_name] = "Y" if has_activity else "N"
    
    # Get all service types and check which ones this location has
    service_types = db.query(ServiceType).all()
    for service_type in service_types:
        if period_id:
            has_service = db.execute(
                text("SELECT 1 FROM location_service_types WHERE location_id = :location_id AND service_type_id = :service_type_id AND period_id = :period_id"),
                {"location_id": location_id, "service_type_id": service_type.service_type_id, "period_id": period_id}
            ).fetchone()
        else:
            has_service = db.execute(
                text("SELECT 1 FROM location_service_types WHERE location_id = :location_id AND service_type_id = :service_type_id"),
                {"location_id": location_id, "service_type_id": service_type.service_type_id}
            ).fetchone()
        
        # Store as original column name with Y/N value
        boolean_columns[service_type.service_type_name] = "Y" if has_service else "N"
    
    # Get all service user bands and check which ones this location has
    user_bands = db.query(ServiceUserBand).all()
    for band in user_bands:
        if period_id:
            has_band = db.execute(
                text("SELECT 1 FROM location_service_user_bands WHERE location_id = :location_id AND band_id = :band_id AND period_id = :period_id"),
                {"location_id": location_id, "band_id": band.band_id, "period_id": period_id}
            ).fetchone()
        else:
            has_band = db.execute(
                text("SELECT 1 FROM location_service_user_bands WHERE location_id = :location_id AND band_id = :band_id"),
                {"location_id": location_id, "band_id": band.band_id}
            ).fetchone()
        
        # Store as original column name with Y/N value
        boolean_columns[band.band_name] = "Y" if has_band else "N"
    
    return boolean_columns


@router.get("/reconstruct-original/{location_id}")
def reconstruct_original_data(
    location_id: str,
    year: Optional[int] = Query(None, description="Year of data period"),
    month: Optional[int] = Query(None, description="Month of data period"),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Reconstruct the original flat CQC data format for a specific location.
    Returns data in the exact format as it appears in the original Excel files.
    
    Args:
        location_id: The CQC location ID (e.g., '1-1000587219')
        year: Optional year filter (e.g., 2025)
        month: Optional month filter (1-12)
    
    Returns:
        Original format data for the location
    """
    try:
        # Build the WHERE clause based on provided filters
        where_conditions = ["l.location_id = :location_id"]
        params = {"location_id": location_id}
        
        if year is not None:
            where_conditions.append("dp.year = :year")
            params["year"] = year
            
        if month is not None:
            where_conditions.append("dp.month = :month") 
            params["month"] = month
            
        where_clause = " AND ".join(where_conditions)
        
        # Comprehensive query to reconstruct original format
        query = text(f"""
            SELECT 
                -- Fields 1-10: Location identification and basic info
                l.location_id,
                l.location_hsca_start_date,
                CASE WHEN lpd.is_dormant THEN 'Y' ELSE 'N' END as is_dormant,
                CASE WHEN lpd.is_care_home THEN 'Y' ELSE 'N' END as is_care_home,
                l.location_name,
                l.location_ods_code,
                l.location_telephone_number,
                lpd.registered_manager,
                lpd.registered_manager_raw,
                lpd.care_homes_beds,
                l.location_type_sector,
                
                -- Fields 11-15: Inspection and rating
                l.location_inspection_directorate,
                l.location_primary_inspection_category, 
                lpd.latest_overall_rating,
                lpd.publication_date,
                CASE WHEN lpd.is_inherited_rating THEN 'Y' ELSE 'N' END as is_inherited_rating,
                
                -- Fields 16-30: Location geography and address
                l.location_region,
                l.location_nhs_region,
                l.location_local_authority,
                l.location_onspd_ccg_code,
                l.location_onspd_ccg,
                l.location_commissioning_ccg_code,
                l.location_commissioning_ccg,
                l.location_street_address,
                l.location_address_line_2,
                l.location_city,
                l.location_county,
                l.location_postal_code,
                l.location_paf_id,
                l.location_uprn_id,
                l.location_latitude,
                l.location_longitude,
                l.location_parliamentary_constituency,
                
                -- Fields 31-40: Provider information
                p.companies_house_number,
                p.brand_id,
                p.provider_id,
                p.provider_name,
                p.hsca_start_date as provider_hsca_start_date,
                p.type_sector as provider_type_sector,
                p.inspection_directorate as provider_inspection_directorate,
                p.primary_inspection_category as provider_primary_inspection_category,
                p.ownership_type,
                p.telephone_number as provider_telephone_number,
                
                -- Fields 41-55: Provider address and contact
                p.web_address as provider_web_address,
                p.street_address as provider_street_address,
                p.address_line_2 as provider_address_line_2,
                p.city as provider_city,
                p.county as provider_county,
                p.postal_code as provider_postal_code,
                p.paf_id as provider_paf_id,
                p.uprn_id as provider_uprn_id,
                p.local_authority as provider_local_authority,
                p.region as provider_region,
                p.nhs_region as provider_nhs_region,
                p.latitude as provider_latitude,
                p.longitude as provider_longitude,
                p.parliamentary_constituency as provider_parliamentary_constituency,
                p.nominated_individual_name,
                p.provider_nominated_individual_name_raw,
                p.provider_main_partner_name,
                p.provider_main_partner_name_raw,
                
                -- Brand information
                b.brand_name,
                
                -- Period information
                dp.year,
                dp.month,
                dp.file_name
                
            FROM locations l
            LEFT JOIN location_period_data lpd ON l.location_id = lpd.location_id
            LEFT JOIN data_periods dp ON lpd.period_id = dp.period_id
            LEFT JOIN providers p ON l.provider_id = p.provider_id
            LEFT JOIN brands b ON p.brand_id = b.brand_id
            WHERE {where_clause}
            ORDER BY dp.year DESC, dp.month DESC
        """)
        
        result = db.execute(query, params).fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail=f"Location {location_id} not found for the specified period")
        
        # Convert result to dictionary
        result_dict = result._asdict()
        
        # Get the period_id for dynamic boolean reconstruction
        period_id = None
        if year is not None or month is not None:
            period_query = text("SELECT period_id FROM data_periods WHERE 1=1")
            period_params = {}
            
            conditions = []
            if year is not None:
                conditions.append("year = :year")
                period_params["year"] = year
            if month is not None:
                conditions.append("month = :month") 
                period_params["month"] = month
                
            if conditions:
                period_query = text(f"SELECT period_id FROM data_periods WHERE {' AND '.join(conditions)}")
                period_result = db.execute(period_query, period_params).fetchone()
                if period_result:
                    period_id = period_result[0]
        
        # Dynamically reconstruct all boolean columns from association tables
        boolean_columns = reconstruct_boolean_columns(db, location_id, period_id)
        
        # Add boolean columns to result
        result_dict.update(boolean_columns)
        
        return {
            "status": "success",
            "location_id": location_id,
            "reconstructed_data": result_dict
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to reconstruct data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to reconstruct data: {str(e)}")


@router.get("/reconstruct-original-flat/{location_id}")  
def reconstruct_original_flat_format(
    location_id: str,
    year: Optional[int] = Query(None),
    month: Optional[int] = Query(None),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Reconstruct the original flat format as a single delimited string,
    exactly matching the format you provided in your example.
    """
    try:
        # Get the reconstructed data
        data_response = reconstruct_original_data(location_id, year, month, db)
        data = data_response["reconstructed_data"]
        
        # Format as the original flat string with tab separations
        flat_fields = [
            str(data.get("location_id", "")),
            str(data.get("location_hsca_start_date", "")),
            str(data.get("is_dormant", "N")),  
            str(data.get("is_care_home", "N")),
            str(data.get("location_name", "")),
            str(data.get("location_ods_code", "")),
            str(data.get("location_telephone_number", "")),
            str(data.get("registered_manager", "")),
            str(data.get("registered_manager_raw", "")),
            str(data.get("care_homes_beds", "")),
            str(data.get("location_type_sector", "")),
            str(data.get("location_inspection_directorate", "")),
            str(data.get("location_primary_inspection_category", "")),
            str(data.get("latest_overall_rating", "")),
            str(data.get("publication_date", "")),
            str(data.get("is_inherited_rating", "N")),
            str(data.get("location_region", "")),
            str(data.get("location_nhs_region", "")),
            str(data.get("location_local_authority", "")),
            str(data.get("location_onspd_ccg_code", "")),
            str(data.get("location_onspd_ccg", "")),
            str(data.get("location_commissioning_ccg_code", "")),
            str(data.get("location_commissioning_ccg", "")),
            str(data.get("location_street_address", "")),
            str(data.get("location_address_line_2", "")),
            str(data.get("location_city", "")),
            str(data.get("location_county", "")),
            str(data.get("location_postal_code", "")),
            str(data.get("location_paf_id", "")),
            str(data.get("location_uprn_id", "")),
            str(data.get("location_latitude", "")),
            str(data.get("location_longitude", "")),
            str(data.get("location_parliamentary_constituency", "")),
            str(data.get("companies_house_number", "")),
            str(data.get("brand_id", "")),
            str(data.get("provider_id", "")),
            str(data.get("provider_name", "")),
            str(data.get("provider_hsca_start_date", "")),
            str(data.get("provider_type_sector", "")),
            str(data.get("provider_inspection_directorate", "")),
            str(data.get("provider_primary_inspection_category", "")),
            str(data.get("ownership_type", "")),
            str(data.get("provider_telephone_number", "")),
            str(data.get("provider_web_address", "")),
            str(data.get("provider_street_address", "")),
            str(data.get("provider_address_line_2", "")),
            str(data.get("provider_city", "")),
            str(data.get("provider_county", "")),
            str(data.get("provider_postal_code", "")),
            str(data.get("provider_paf_id", "")),
            str(data.get("provider_uprn_id", "")),
            str(data.get("provider_local_authority", "")),
            str(data.get("provider_region", "")),
            str(data.get("provider_nhs_region", "")),
            str(data.get("provider_latitude", "")),
            str(data.get("provider_longitude", "")),
            str(data.get("provider_parliamentary_constituency", "")),
            str(data.get("nominated_individual_name", "")),
            str(data.get("provider_nominated_individual_name_raw", "")),
            str(data.get("provider_main_partner_name", "*")),
            str(data.get("provider_main_partner_name_raw", "")),
            # Activity flags (Y/N values)
            "Y" if data.get("accommodation_nursing_personal_care") else "",
            "Y" if data.get("treatment_disease_disorder_injury") else "",
            # Add more activity flags as needed...
        ]
        
        # Join with multiple spaces to match your format
        flat_string = "    ".join(flat_fields)
        
        return {
            "status": "success", 
            "location_id": location_id,
            "original_flat_format": flat_string,
            "field_count": len(flat_fields)
        }
        
    except Exception as e:
        logger.error(f"Failed to create flat format: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to create flat format: {str(e)}")