from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text, and_, or_
from typing import Dict, Any, List, Optional, Union
from pydantic import BaseModel, Field
import logging
import json
from app.core.database import get_db

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


# Define all available columns for filtering
AVAILABLE_COLUMNS = {
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

    # Location geography and address
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
    "companies_house_number": "p.companies_house_number",
    "charity_number": "p.charity_number",
    "brand_id": "p.brand_id",
    "provider_id": "p.provider_id",
    "provider_name": "p.provider_name",
    "provider_hsca_start_date": "p.hsca_start_date",
    "provider_type_sector": "p.type_sector",
    "provider_inspection_directorate": "p.inspection_directorate",
    "provider_primary_inspection_category": "p.primary_inspection_category",
    "ownership_type": "p.ownership_type",
    "provider_telephone_number": "p.telephone_number",
    "provider_web_address": "p.web_address",
    "provider_street_address": "p.street_address",
    "provider_address_line_2": "p.address_line_2",
    "provider_city": "p.city",
    "provider_county": "p.county",
    "provider_postal_code": "p.postal_code",
    "provider_paf_id": "p.paf_id",
    "provider_uprn_id": "p.uprn_id",
    "provider_local_authority": "p.local_authority",
    "provider_region": "p.region",
    "provider_nhs_region": "p.nhs_region",
    "provider_latitude": "p.latitude",
    "provider_longitude": "p.longitude",
    "provider_parliamentary_constituency": "p.parliamentary_constituency",
    "nominated_individual_name": "p.nominated_individual_name",
    "main_partner_name": "p.main_partner_name",

    # Brand information
    "brand_name": "b.brand_name",

    # Activity flags - Regulated Activities
    "accommodation_nursing_personal_care": "laf.accommodation_nursing_personal_care",
    "treatment_disease_disorder_injury": "laf.treatment_disease_disorder_injury",
    "assessment_medical_treatment": "laf.assessment_medical_treatment",
    "surgical_procedures": "laf.surgical_procedures",
    "diagnostic_screening": "laf.diagnostic_screening",
    "management_supply_blood": "laf.management_supply_blood",
    "transport_services": "laf.transport_services",
    "maternity_midwifery": "laf.maternity_midwifery",
    "termination_pregnancies": "laf.termination_pregnancies",
    "services_slimming": "laf.services_slimming",
    "nursing_care": "laf.nursing_care",
    "personal_care": "laf.personal_care",
    "accommodation_persons_detoxification": "laf.accommodation_persons_detoxification",
    "accommodation_persons_past_present_alcohol_dependence": "laf.accommodation_persons_past_present_alcohol_dependence",
    "family_planning": "laf.family_planning",

    # Service Types - Complete list from CQC data
    "acute_services_with_overnight_beds": "laf.acute_services_with_overnight_beds",
    "acute_services_without_overnight_beds": "laf.acute_services_without_overnight_beds",
    "ambulance_service": "laf.ambulance_service",
    "blood_and_transplant_service": "laf.blood_and_transplant_service",
    "care_home_nursing": "laf.care_home_nursing",
    "care_home_without_nursing": "laf.care_home_without_nursing",
    "community_based_services_substance_misuse": "laf.community_based_services_substance_misuse",
    "community_based_services_learning_disability": "laf.community_based_services_learning_disability", 
    "community_based_services_mental_health": "laf.community_based_services_mental_health",
    "community_health_care_independent_midwives": "laf.community_health_care_independent_midwives",
    "community_health_care_nurses_agency": "laf.community_health_care_nurses_agency",
    "community_health_care": "laf.community_health_care",
    "dental_service": "laf.dental_service",
    "diagnostic_screening_service": "laf.diagnostic_screening_service",
    "diagnostic_screening_single_handed_sessional": "laf.diagnostic_screening_single_handed_sessional",
    "doctors_consultation": "laf.doctors_consultation",
    "doctors_treatment": "laf.doctors_treatment", 
    "domiciliary_care": "laf.domiciliary_care",
    "extra_care_housing": "laf.extra_care_housing",
    "hospice_services": "laf.hospice_services",
    "hospice_services_at_home": "laf.hospice_services_at_home",
    "hospital_services_mental_health_learning_disabilities": "laf.hospital_services_mental_health_learning_disabilities",
    "hospital_services_acute": "laf.hospital_services_acute",
    "hyperbaric_chamber": "laf.hyperbaric_chamber",
    "long_term_conditions": "laf.long_term_conditions",
    "mobile_doctors": "laf.mobile_doctors",
    "prison_healthcare": "laf.prison_healthcare",
    "rehabilitation_services": "laf.rehabilitation_services",
    "remote_clinical_advice": "laf.remote_clinical_advice",
    "residential_substance_misuse_treatment": "laf.residential_substance_misuse_treatment",
    "shared_lives": "laf.shared_lives",
    "specialist_college": "laf.specialist_college",
    "supported_living": "laf.supported_living", 
    "urgent_care": "laf.urgent_care",

    # Service User Bands - Complete list from CQC data
    "children_0_18_years": "laf.children_0_18_years",
    "dementia": "laf.dementia", 
    "learning_disabilities_autistic": "laf.learning_disabilities_autistic",
    "mental_health_needs": "laf.mental_health_needs",
    "older_people_65_plus": "laf.older_people_65_plus",
    "people_detained_mental_health_act": "laf.people_detained_mental_health_act",
    "people_who_misuse_drugs_alcohol": "laf.people_who_misuse_drugs_alcohol",
    "people_with_eating_disorder": "laf.people_with_eating_disorder",
    "physical_disability": "laf.physical_disability",
    "sensory_impairment": "laf.sensory_impairment",
    "whole_population": "laf.whole_population",
    "younger_adults": "laf.younger_adults",
    
    # Legacy backward compatibility fields
    "children_0_3_years": "laf.children_0_3_years",
    "children_4_12_years": "laf.children_4_12_years",
    "children_13_18_years": "laf.children_13_18_years",
    "adults_18_65_years": "laf.adults_18_65_years",

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


def build_filter_condition(filter_cond: FilterCondition, params: Dict[str, Any], param_counter: int) -> str:
    """Build a single filter condition for the WHERE clause"""
    if filter_cond.column not in AVAILABLE_COLUMNS:
        raise HTTPException(status_code=400, detail=f"Column '{filter_cond.column}' not available for filtering")

    column_sql = AVAILABLE_COLUMNS[filter_cond.column]
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

        # Boolean service flags (true/false or 1/0)
        domiciliary_care: Optional[bool] = Query(None, description="Filter locations with domiciliary care"),
        care_home_nursing: Optional[bool] = Query(None, description="Filter nursing care homes"),
        care_home_without_nursing: Optional[bool] = Query(None, description="Filter care homes without nursing"),
        hospital_services_acute: Optional[bool] = Query(None, description="Filter acute hospital services"),
        community_health_care: Optional[bool] = Query(None, description="Filter community health care"),
        older_people_65_plus: Optional[bool] = Query(None, description="Filter services for elderly (65+)"),
        dementia: Optional[bool] = Query(None, description="Filter dementia care services"),
        mental_health_needs: Optional[bool] = Query(None, description="Filter mental health services"),
        learning_disabilities_autistic: Optional[bool] = Query(None,
                                                               description="Filter learning disability/autism services"),
        physical_disability: Optional[bool] = Query(None, description="Filter physical disability services"),

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

        # Boolean service flags
        boolean_filters = {
            "domiciliary_care": domiciliary_care,
            "care_home_nursing": care_home_nursing,
            "care_home_without_nursing": care_home_without_nursing,
            "hospital_services_acute": hospital_services_acute,
            "community_health_care": community_health_care,
            "older_people_65_plus": older_people_65_plus,
            "dementia": dementia,
            "mental_health_needs": mental_health_needs,
            "learning_disabilities_autistic": learning_disabilities_autistic,
            "physical_disability": physical_disability,
        }

        for column, value in boolean_filters.items():
            if value is not None:
                individual_filters.append(FilterCondition(column=column, value=value, operator="equals"))

        # Process individual filters
        for filter_cond in individual_filters:
            condition = build_filter_condition(filter_cond, params, param_counter)
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
            LEFT JOIN brands b ON p.brand_id = b.brand_id
            LEFT JOIN location_activity_flags laf ON l.location_id = laf.location_id AND lpd.period_id = laf.period_id
            LEFT JOIN dual_registrations dr ON l.location_id = dr.location_id AND dp.year = dr.year AND dp.month = dr.month
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
            LEFT JOIN brands b ON p.brand_id = b.brand_id
            LEFT JOIN location_activity_flags laf ON l.location_id = laf.location_id AND lpd.period_id = laf.period_id
            LEFT JOIN dual_registrations dr ON l.location_id = dr.location_id AND dp.year = dr.year AND dp.month = dr.month
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
def get_available_columns() -> Dict[str, List[str]]:
    """
    Get list of all available columns for filtering
    """
    return {
        "available_columns": list(AVAILABLE_COLUMNS.keys()),
        "total_columns": len(AVAILABLE_COLUMNS),
        "categories": {
            "location_info": [col for col in AVAILABLE_COLUMNS.keys() if col.startswith("location_")],
            "provider_info": [col for col in AVAILABLE_COLUMNS.keys() if col.startswith("provider_")],
            "activity_flags": [col for col in AVAILABLE_COLUMNS.keys() if col in [
                "accommodation_nursing_personal_care", "treatment_disease_disorder_injury",
                "care_home_nursing", "care_home_without_nursing", "domiciliary_care",
                "hospital_services_acute", "community_health_care"
            ]],
            "user_bands": [col for col in AVAILABLE_COLUMNS.keys() if col in [
                "older_people_65_plus", "dementia", "learning_disabilities_autistic",
                "mental_health_needs", "physical_disability", "children_0_3_years",
                "children_4_12_years", "children_13_18_years", "adults_18_65_years"
            ]],
            "general": ["year", "month", "file_name", "brand_name", "is_dormant", "is_care_home", 
                       "is_dual_registered", "dual_linked_organisation_id", "dual_relationship_type", 
                       "dual_relationship_start_date", "is_primary_in_dual"]
        }
    }


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