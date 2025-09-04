from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text, and_, or_
from typing import Dict, Any, List, Optional, Union
from pydantic import BaseModel, Field
import logging
import json
from app.core.database import get_db
from app.models.regulated_activity import RegulatedActivity
from app.models.service_type import ServiceType  
from app.models.service_user_band import ServiceUserBand

router = APIRouter()
logger = logging.getLogger(__name__)


class FilterCondition(BaseModel):
    """Single filter condition"""
    column: str = Field(..., description="Column name to filter on")
    value: Union[str, int, float, bool] = Field(..., description="Value to filter by")
    operator: str = Field(default="equals",
                          description="Operator: equals, not_equals, contains, starts_with, ends_with, gt, gte, lt, lte, in, not_in")
    case_sensitive: bool = Field(default=False, description="Case sensitive matching for string operations")


class FilterRequest(BaseModel):
    """Multiple filter conditions with logic"""
    filters: List[FilterCondition] = Field(..., description="List of filter conditions")
    logic: str = Field(default="AND", description="Logic operator between conditions: AND or OR")
    limit: Optional[int] = Field(default=100, description="Maximum number of records to return")
    offset: Optional[int] = Field(default=0, description="Number of records to skip")
    order_by: Optional[str] = Field(default=None, description="Column to order by")
    order_direction: str = Field(default="ASC", description="Order direction: ASC or DESC")


def get_dynamic_available_columns(db: Session) -> Dict[str, str]:
    """
    Dynamically build available columns by querying the database.
    This ensures we always have the latest column definitions.
    """
    # Base static columns (non-boolean)
    base_columns = {
        # Location identification and basic info
        "location_id": "l.location_id",
        "location_hsca_start_date": "l.location_hsca_start_date",
        "is_dormant": "lpd.is_dormant",
        "is_care_home": "lpd.is_care_home",
        "location_name": "l.location_name",
        "location_ods_code": "l.location_ods_code",
        "location_telephone_number": "l.location_telephone_number",
        "registered_manager": "lpd.registered_manager",
        "care_homes_beds": "lpd.care_homes_beds",
        "location_type_sector": "l.location_type_sector",
        
        # Inspection and rating
        "location_inspection_directorate": "l.location_inspection_directorate",
        "location_primary_inspection_category": "l.location_primary_inspection_category",
        "latest_overall_rating": "lpd.latest_overall_rating",
        "publication_date": "lpd.publication_date",
        "is_inherited_rating": "lpd.is_inherited_rating",

        # Geographic information
        "location_region": "l.location_region",
        "location_nhs_region": "l.location_nhs_region",
        "location_local_authority": "l.location_local_authority",
        "location_onspd_ccg_code": "l.location_onspd_ccg_code",
        "location_onspd_ccg": "l.location_onspd_ccg",
        "location_commissioning_ccg_code": "l.location_commissioning_ccg_code",
        "location_commissioning_ccg": "l.location_commissioning_ccg",
        "location_street_address": "l.location_street_address",
        "location_address_line_2": "l.location_address_line_2",
        "location_city": "l.location_city",
        "location_county": "l.location_county",
        "location_postal_code": "l.location_postal_code",
        "location_paf_id": "l.location_paf_id",
        "location_uprn_id": "l.location_uprn_id",
        "location_latitude": "l.location_latitude",
        "location_longitude": "l.location_longitude",
        "location_parliamentary_constituency": "l.location_parliamentary_constituency",
        "location_also_known_as": "l.location_also_known_as",
        "location_specialisms": "l.location_specialisms",
        "location_web_address": "l.location_web_address",

        # Provider information  
        "provider_id": "p.provider_id",
        "provider_name": "p.provider_name",
        "provider_hsca_start_date": "p.provider_hsca_start_date",
        "provider_companies_house_number": "p.provider_companies_house_number", 
        "provider_charity_number": "p.provider_charity_number",
        "provider_type_sector": "p.provider_type_sector",
        "provider_inspection_directorate": "p.provider_inspection_directorate",
        "provider_primary_inspection_category": "p.provider_primary_inspection_category",
        "provider_ownership_type": "p.provider_ownership_type",
        "provider_telephone_number": "p.provider_telephone_number",
        "provider_web_address": "p.provider_web_address",
        "provider_street_address": "p.provider_street_address",
        "provider_address_line_2": "p.provider_address_line_2",
        "provider_city": "p.provider_city",
        "provider_county": "p.provider_county",
        "provider_postal_code": "p.provider_postal_code",
        "provider_paf_id": "p.provider_paf_id",
        "provider_uprn_id": "p.provider_uprn_id",
        "provider_local_authority": "p.provider_local_authority",
        "provider_region": "p.provider_region",
        "provider_nhs_region": "p.provider_nhs_region",
        "provider_latitude": "p.provider_latitude",
        "provider_longitude": "p.provider_longitude",
        "provider_parliamentary_constituency": "p.provider_parliamentary_constituency",
        "provider_nominated_individual_name": "p.provider_nominated_individual_name",
        "provider_main_partner_name": "p.provider_main_partner_name",

        # Brand information
        "brand_name": "b.brand_name",

        # Period information
        "year": "dp.year",
        "month": "dp.month",
        "file_name": "dp.file_name",

        # Dual registration information
        "is_dual_registered": "CASE WHEN dr.location_id IS NOT NULL THEN true ELSE false END",
        "dual_linked_organisation_id": "dr.linked_organisation_id",
        "dual_relationship_type": "dr.relationship_type",
        "dual_relationship_start_date": "dr.relationship_start_date",
        "is_primary_in_dual": "dr.is_primary"
    }
    
    # Dynamically add regulated activities
    activities = db.query(RegulatedActivity).all()
    for activity in activities:
        # Create a safe column name from the full activity name
        safe_name = activity.activity_name.lower().replace(' ', '_').replace('-', '_').replace('/', '_').replace('(', '').replace(')', '').replace(',', '').replace('.', '').replace("'", '').replace('"', '')
        # Use EXISTS subquery to check if location has this activity for the current period
        base_columns[f"has_activity_{safe_name}"] = f"EXISTS (SELECT 1 FROM location_regulated_activities lra WHERE lra.location_id = l.location_id AND lra.activity_id = {activity.activity_id} AND lra.period_id = lpd.period_id)"
    
    # Dynamically add service types
    service_types = db.query(ServiceType).all()
    for service_type in service_types:
        # Create a safe column name from the full service type name
        safe_name = service_type.service_type_name.lower().replace(' ', '_').replace('-', '_').replace('/', '_').replace('(', '').replace(')', '').replace(',', '').replace('.', '').replace("'", '').replace('"', '')
        # Use EXISTS subquery to check if location has this service type for the current period
        base_columns[f"has_service_{safe_name}"] = f"EXISTS (SELECT 1 FROM location_service_types lst WHERE lst.location_id = l.location_id AND lst.service_type_id = {service_type.service_type_id} AND lst.period_id = lpd.period_id)"
    
    # Dynamically add service user bands
    user_bands = db.query(ServiceUserBand).all()
    for band in user_bands:
        # Create a safe column name from the full band name
        safe_name = band.band_name.lower().replace(' ', '_').replace('-', '_').replace('/', '_').replace('(', '').replace(')', '').replace(',', '').replace('.', '').replace("'", '').replace('"', '')
        # Use EXISTS subquery to check if location has this user band for the current period
        base_columns[f"has_band_{safe_name}"] = f"EXISTS (SELECT 1 FROM location_service_user_bands lsub WHERE lsub.location_id = l.location_id AND lsub.band_id = {band.band_id} AND lsub.period_id = lpd.period_id)"
    
    return base_columns


# FILTERING SYSTEM NOW 100% DYNAMIC
# =====================================
# All column definitions are dynamically generated from the database via get_dynamic_available_columns().
# No hardcoded boolean columns or activity mappings. 
# 
# Boolean filtering now uses EXISTS subqueries to association tables:
# - Regulated Activities: has_activity_<name> -> location_regulated_activities table
# - Service Types: has_service_<name> -> location_service_types table  
# - Service User Bands: has_band_<name> -> location_service_user_bands table
#
# This ensures the system automatically adapts to new CQC data columns without code changes.


def build_filter_condition(filter_cond: FilterCondition, params: Dict[str, Any], param_counter: int, available_columns: Dict[str, str]) -> str:
    """Build a single filter condition for the WHERE clause"""
    if filter_cond.column not in available_columns:
        raise HTTPException(status_code=400, detail=f"Column '{filter_cond.column}' not available for filtering")

    column_sql = available_columns[filter_cond.column]
    param_name = f"param_{param_counter}"

    if filter_cond.operator == "equals":
        params[param_name] = filter_cond.value
        return f"{column_sql} = :{param_name}"

    elif filter_cond.operator == "not_equals":
        params[param_name] = filter_cond.value
        return f"{column_sql} != :{param_name}"

    elif filter_cond.operator == "contains":
        if filter_cond.case_sensitive:
            params[param_name] = f"%{filter_cond.value}%"
            return f"{column_sql} LIKE :{param_name}"
        else:
            params[param_name] = f"%{str(filter_cond.value).lower()}%"
            return f"LOWER({column_sql}) LIKE :{param_name}"

    elif filter_cond.operator == "starts_with":
        if filter_cond.case_sensitive:
            params[param_name] = f"{filter_cond.value}%"
            return f"{column_sql} LIKE :{param_name}"
        else:
            params[param_name] = f"{str(filter_cond.value).lower()}%"
            return f"LOWER({column_sql}) LIKE :{param_name}"

    elif filter_cond.operator == "ends_with":
        if filter_cond.case_sensitive:
            params[param_name] = f"%{filter_cond.value}"
            return f"{column_sql} LIKE :{param_name}"
        else:
            params[param_name] = f"%{str(filter_cond.value).lower()}"
            return f"LOWER({column_sql}) LIKE :{param_name}"

    elif filter_cond.operator == "gt":
        params[param_name] = filter_cond.value
        return f"{column_sql} > :{param_name}"

    elif filter_cond.operator == "gte":
        params[param_name] = filter_cond.value
        return f"{column_sql} >= :{param_name}"

    elif filter_cond.operator == "lt":
        params[param_name] = filter_cond.value
        return f"{column_sql} < :{param_name}"

    elif filter_cond.operator == "lte":
        params[param_name] = filter_cond.value
        return f"{column_sql} <= :{param_name}"

    elif filter_cond.operator == "in":
        if isinstance(filter_cond.value, str):
            # Split comma-separated values
            values = [v.strip() for v in filter_cond.value.split(",")]
        else:
            values = filter_cond.value if isinstance(filter_cond.value, list) else [filter_cond.value]

        placeholders = []
        for i, value in enumerate(values):
            param_key = f"{param_name}_{i}"
            params[param_key] = value
            placeholders.append(f":{param_key}")

        return f"{column_sql} IN ({','.join(placeholders)})"

    elif filter_cond.operator == "not_in":
        if isinstance(filter_cond.value, str):
            values = [v.strip() for v in filter_cond.value.split(",")]
        else:
            values = filter_cond.value if isinstance(filter_cond.value, list) else [filter_cond.value]

        placeholders = []
        for i, value in enumerate(values):
            param_key = f"{param_name}_{i}"
            params[param_key] = value
            placeholders.append(f":{param_key}")

        return f"{column_sql} NOT IN ({','.join(placeholders)})"

    else:
        raise HTTPException(status_code=400, detail=f"Operator '{filter_cond.operator}' not supported")


@router.get("/filter-data")
def filter_cqc_data(
        # Column selection parameter
        fields: Optional[str] = Query(None, description="Comma-separated list of fields to return"),

        # Filter parameters (support multiple filters using JSON or repeated params)
        filters: Optional[str] = Query(None,
                                       description="JSON string of filter conditions or use individual filter params"),

        # Individual filter parameters for simple queries
        location_id: Optional[str] = Query(None, description="Filter by location ID"),
        location_name: Optional[str] = Query(None, description="Filter by location name (contains)"),
        location_city: Optional[str] = Query(None, description="Filter by city"),
        location_region: Optional[str] = Query(None, description="Filter by region"),
        location_postal_code: Optional[str] = Query(None, description="Filter by postal code"),
        provider_name: Optional[str] = Query(None, description="Filter by provider name (contains)"),
        is_care_home: Optional[str] = Query(None, description="Filter by care home status (Y/N)"),
        is_dormant: Optional[str] = Query(None, description="Filter by dormant status (Y/N)"),
        latest_overall_rating: Optional[str] = Query(None,
                                                     description="Filter by rating (exact match or comma-separated list)"),
        care_homes_beds_min: Optional[int] = Query(None, description="Minimum number of care home beds"),
        care_homes_beds_max: Optional[int] = Query(None, description="Maximum number of care home beds"),
        year: Optional[int] = Query(None, description="Filter by year"),
        month: Optional[int] = Query(None, description="Filter by month"),

        # Dynamic boolean filters - use the format: activity_<safe_name>=true, service_<safe_name>=true, band_<safe_name>=true
        # Examples: has_activity_accommodation_for_persons_who_require_nursing_or_personal_care=true
        #          has_service_care_home_service_with_nursing=true
        #          has_band_older_people=true

        # Query parameters
        logic: str = Query("AND", description="Logic operator between conditions: AND or OR"),
        limit: int = Query(100, description="Maximum number of records to return", ge=1, le=1000),
        offset: int = Query(0, description="Number of records to skip", ge=0),
        order_by: Optional[str] = Query(None, description="Column to order by"),
        order_direction: str = Query("ASC", description="Order direction: ASC or DESC"),

        db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Filter CQC data with flexible conditions using query parameters

    Examples:

    1. Simple filters with field selection:
       GET /filter-data?fields=location_id,location_name,provider_name&location_city=London

    2. Multiple values with specific columns:
       GET /filter-data?fields=location_name,latest_overall_rating&latest_overall_rating=Outstanding,Good

    3. All columns (default behavior):
       GET /filter-data?location_city=London

    4. Complex JSON filters with field selection:
       GET /filter-data?fields=location_id,location_name&filters=[{"column":"provider_name","value":"Healthcare","operator":"contains"}]
    """
    
    try:
        # Get dynamic columns based on current database state
        AVAILABLE_COLUMNS = get_dynamic_available_columns(db)
    except Exception as e:
        logger.error(f"Error getting dynamic columns: {e}")
        # Create minimal fallback with only basic non-boolean columns if dynamic fails
        AVAILABLE_COLUMNS = {
            # Basic location and provider columns only - no boolean columns as fallback
            "location_id": "l.location_id",
            "location_name": "l.location_name", 
            "location_city": "l.location_city",
            "location_region": "l.location_region",
            "provider_id": "p.provider_id",
            "provider_name": "p.provider_name",
            "latest_overall_rating": "lpd.latest_overall_rating",
            "year": "dp.year",
            "month": "dp.month",
            "is_care_home": "lpd.is_care_home",
            "is_dormant": "lpd.is_dormant"
        }
        logger.warning("Using minimal fallback columns - boolean filtering disabled")
    try:
        # Build WHERE conditions
        where_conditions = []
        params = {}
        param_counter = 0

        # Handle JSON filters if provided
        if filters:
            try:
                json_filters = json.loads(filters)
                for filter_data in json_filters:
                    filter_cond = FilterCondition(**filter_data)
                    condition = build_filter_condition(filter_cond, params, param_counter)
                    where_conditions.append(condition)
                    param_counter += 1
            except json.JSONDecodeError:
                raise HTTPException(status_code=400, detail="Invalid JSON in filters parameter")
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Invalid filter format: {str(e)}")

        # Handle individual query parameters
        individual_filters = []

        # Simple exact match filters
        if location_id:
            individual_filters.append(FilterCondition(column="location_id", value=location_id, operator="equals"))
        if location_city:
            individual_filters.append(FilterCondition(column="location_city", value=location_city, operator="equals"))
        if location_region:
            if "," in location_region:
                individual_filters.append(
                    FilterCondition(column="location_region", value=location_region, operator="in"))
            else:
                individual_filters.append(
                    FilterCondition(column="location_region", value=location_region, operator="equals"))
        if location_postal_code:
            individual_filters.append(
                FilterCondition(column="location_postal_code", value=location_postal_code, operator="equals"))
        if is_care_home:
            individual_filters.append(
                FilterCondition(column="is_care_home", value=is_care_home.upper(), operator="equals"))
        if is_dormant:
            individual_filters.append(FilterCondition(column="is_dormant", value=is_dormant.upper(), operator="equals"))
        if year:
            individual_filters.append(FilterCondition(column="year", value=year, operator="equals"))
        if month:
            individual_filters.append(FilterCondition(column="month", value=month, operator="equals"))

        # Contains filters
        if location_name:
            individual_filters.append(
                FilterCondition(column="location_name", value=location_name, operator="contains", case_sensitive=False))
        if provider_name:
            individual_filters.append(
                FilterCondition(column="provider_name", value=provider_name, operator="contains", case_sensitive=False))

        # Rating filter (supports multiple values)
        if latest_overall_rating:
            if "," in latest_overall_rating:
                individual_filters.append(
                    FilterCondition(column="latest_overall_rating", value=latest_overall_rating, operator="in"))
            else:
                individual_filters.append(
                    FilterCondition(column="latest_overall_rating", value=latest_overall_rating, operator="equals"))

        # Numeric range filters
        if care_homes_beds_min:
            individual_filters.append(
                FilterCondition(column="care_homes_beds", value=care_homes_beds_min, operator="gte"))
        if care_homes_beds_max:
            individual_filters.append(
                FilterCondition(column="care_homes_beds", value=care_homes_beds_max, operator="lte"))

        # Dynamic boolean filtering is now handled through the standard filters parameter
        # Users can filter using column names like: has_activity_accommodation_nursing_personal_care=true

        # Process individual filters
        for filter_cond in individual_filters:
            condition = build_filter_condition(filter_cond, params, param_counter, AVAILABLE_COLUMNS)
            where_conditions.append(condition)
            param_counter += 1

        if not where_conditions:
            raise HTTPException(status_code=400, detail="At least one filter condition is required")

        # Combine conditions with logic operator
        logic_operator = " AND " if logic.upper() == "AND" else " OR "
        where_clause = f"({logic_operator.join(where_conditions)})"

        # Build ORDER BY clause
        order_clause = ""
        if order_by:
            if order_by not in AVAILABLE_COLUMNS:
                raise HTTPException(status_code=400, detail=f"Column '{order_by}' not available for ordering")

            order_column = AVAILABLE_COLUMNS[order_by]
            order_dir = order_direction.upper()
            if order_dir not in ["ASC", "DESC"]:
                order_dir = "ASC"
            order_clause = f"ORDER BY {order_column} {order_dir}"
        else:
            order_clause = "ORDER BY l.location_id ASC"

        # Handle field selection (projection)
        if fields:
            requested_columns = [col.strip() for col in fields.split(",")]
            invalid_columns = set(requested_columns) - set(AVAILABLE_COLUMNS.keys())

            if invalid_columns:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid columns requested: {', '.join(invalid_columns)}"
                )

            # Build select clause with only requested columns
            select_columns = [f"{AVAILABLE_COLUMNS[col]} as {col}" for col in requested_columns]
            select_clause = ", ".join(select_columns)
        else:
            # Default to all columns
            select_columns = [f"{expr} as {col}" for col, expr in AVAILABLE_COLUMNS.items()]
            select_clause = ", ".join(select_columns)

        # Build complete query with dynamic SELECT
        query = text(f"""
            SELECT 
                {select_clause}
            FROM locations l
            LEFT JOIN location_period_data lpd ON l.location_id = lpd.location_id
            LEFT JOIN data_periods dp ON lpd.period_id = dp.period_id
            LEFT JOIN providers p ON l.provider_id = p.provider_id
            LEFT JOIN provider_brands pb ON p.provider_id = pb.provider_id AND lpd.period_id = pb.period_id
            LEFT JOIN brands b ON pb.brand_id = b.brand_id
            LEFT JOIN dual_registrations dr ON l.location_id = dr.location_id AND lpd.period_id = dr.period_id
            WHERE {where_clause}
            {order_clause}
            LIMIT :limit OFFSET :offset
        """)

        # Add pagination parameters
        params['limit'] = limit
        params['offset'] = offset

        # Execute query
        results = db.execute(query, params).fetchall()

        # Get total count for pagination
        count_query = text(f"""
            SELECT COUNT(DISTINCT l.location_id) as total_count
            FROM locations l
            LEFT JOIN location_period_data lpd ON l.location_id = lpd.location_id
            LEFT JOIN data_periods dp ON lpd.period_id = dp.period_id
            LEFT JOIN providers p ON l.provider_id = p.provider_id
            LEFT JOIN provider_brands pb ON p.provider_id = pb.provider_id AND lpd.period_id = pb.period_id
            LEFT JOIN brands b ON pb.brand_id = b.brand_id
            LEFT JOIN dual_registrations dr ON l.location_id = dr.location_id AND lpd.period_id = dr.period_id
            WHERE {where_clause}
        """)

        count_params = {k: v for k, v in params.items() if k not in ['limit', 'offset']}
        total_count = db.execute(count_query, count_params).fetchone()[0]

        # Convert results to dictionaries
        data = [dict(row._mapping) for row in results]

        return {
            "status": "success",
            "data": data,
            "pagination": {
                "total_count": total_count,
                "limit": limit,
                "offset": offset,
                "current_page": (offset // limit) + 1,
                "total_pages": (total_count + limit - 1) // limit
            },
            "filters_applied": {
                "conditions": len(where_conditions),
                "logic": logic
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to filter data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to filter data: {str(e)}")


@router.get("/available-columns")
def get_available_columns(db: Session = Depends(get_db)) -> Dict[str, Any]:
    """
    Get list of all available columns for filtering, dynamically generated from database
    """
    try:
        # Get dynamic columns from database
        available_columns = get_dynamic_available_columns(db)
        
        return {
            "available_columns": list(available_columns.keys()),
            "total_columns": len(available_columns),
            "categories": {
                "location_info": [col for col in available_columns.keys() if col.startswith("location_")],
                "provider_info": [col for col in available_columns.keys() if col.startswith("provider_")],
                "regulated_activities": [col for col in available_columns.keys() if col.startswith("has_activity_")],
                "service_types": [col for col in available_columns.keys() if col.startswith("has_service_")],
                "service_user_bands": [col for col in available_columns.keys() if col.startswith("has_band_")],
                "period_info": [col for col in available_columns.keys() if col in ["year", "month", "file_name"]],
                "ratings_and_status": [col for col in available_columns.keys() if col in [
                    "latest_overall_rating", "publication_date", "is_inherited_rating", 
                    "is_dormant", "is_care_home", "care_homes_beds"
                ]],
                "dual_registrations": [col for col in available_columns.keys() if col.startswith("dual_") or col == "is_dual_registered"],
                "brands": [col for col in available_columns.keys() if col.startswith("brand_")]
            },
            "examples": {
                "filter_by_activity": "has_activity_accommodation_for_persons_who_require_nursing_or_personal_care=true",
                "filter_by_service": "has_service_care_home_service_with_nursing=true", 
                "filter_by_user_band": "has_band_older_people=true",
                "filter_by_location": "location_city=London",
                "filter_by_rating": "latest_overall_rating=Outstanding"
            }
        }
    except Exception as e:
        logger.error(f"Error getting available columns: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get available columns: {str(e)}")


@router.get("/boolean-filters")
def get_available_boolean_filters(db: Session = Depends(get_db)) -> Dict[str, Any]:
    """
    Get all available boolean filters with their actual database names and descriptions.
    This shows what regulated activities, service types, and service user bands are available for filtering.
    """
    try:
        result = {
            "regulated_activities": [],
            "service_types": [], 
            "service_user_bands": [],
            "usage_examples": []
        }
        
        # Get all regulated activities
        activities = db.query(RegulatedActivity).all()
        for activity in activities:
            safe_name = activity.activity_name.lower().replace(' ', '_').replace('-', '_').replace('/', '_').replace('(', '').replace(')', '').replace(',', '').replace('.', '').replace("'", '').replace('"', '')
            result["regulated_activities"].append({
                "full_name": activity.activity_name,
                "filter_column": f"has_activity_{safe_name}",
                "example_usage": f"has_activity_{safe_name}=true"
            })
        
        # Get all service types
        service_types = db.query(ServiceType).all()
        for service_type in service_types:
            safe_name = service_type.service_type_name.lower().replace(' ', '_').replace('-', '_').replace('/', '_').replace('(', '').replace(')', '').replace(',', '').replace('.', '').replace("'", '').replace('"', '')
            result["service_types"].append({
                "full_name": service_type.service_type_name,
                "filter_column": f"has_service_{safe_name}",
                "example_usage": f"has_service_{safe_name}=true"
            })
        
        # Get all service user bands
        user_bands = db.query(ServiceUserBand).all()
        for band in user_bands:
            safe_name = band.band_name.lower().replace(' ', '_').replace('-', '_').replace('/', '_').replace('(', '').replace(')', '').replace(',', '').replace('.', '').replace("'", '').replace('"', '')
            result["service_user_bands"].append({
                "full_name": band.band_name,
                "filter_column": f"has_band_{safe_name}",
                "example_usage": f"has_band_{safe_name}=true"
            })
        
        # Add usage examples
        if result["regulated_activities"]:
            result["usage_examples"].append(f"Filter for nursing care: {result['regulated_activities'][0]['example_usage']}")
        if result["service_types"]:
            result["usage_examples"].append(f"Filter for specific services: {result['service_types'][0]['example_usage']}")
        if result["service_user_bands"]:
            result["usage_examples"].append(f"Filter for user demographics: {result['service_user_bands'][0]['example_usage']}")
        
        result["summary"] = {
            "total_regulated_activities": len(result["regulated_activities"]),
            "total_service_types": len(result["service_types"]),
            "total_service_user_bands": len(result["service_user_bands"])
        }
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting boolean filters: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get boolean filters: {str(e)}")


@router.get("/operators")
def get_available_operators() -> Dict[str, Any]:
    """
    Get list of available filter operators and their descriptions
    """
    return {
        "operators": {
            "equals": "Exact match (=)",
            "not_equals": "Not equal (!=)",
            "contains": "Contains substring (LIKE %value%)",
            "starts_with": "Starts with (LIKE value%)",
            "ends_with": "Ends with (LIKE %value)",
            "gt": "Greater than (>)",
            "gte": "Greater than or equal (>=)",
            "lt": "Less than (<)",
            "lte": "Less than or equal (<=)",
            "in": "In list (value1,value2,value3)",
            "not_in": "Not in list (NOT IN)"
        },
        "logic_operators": ["AND", "OR"],
        "order_directions": ["ASC", "DESC"]
    }