from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from app.core.database import get_db
from app.models.location import Location as LocationModel
from app.schemas.location import Location, LocationCreate, LocationPeriodData
from app.models.location_period_data import LocationPeriodData as LocationPeriodDataModel
from app.models.data_period import DataPeriod

router = APIRouter()


@router.get("/", response_model=List[Location])
def get_locations(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    location_region: Optional[str] = None,
    location_local_authority: Optional[str] = None,
    is_care_home: Optional[bool] = None,
    latest_overall_rating: Optional[str] = None,
    location_type_sector: Optional[str] = None,
    year: Optional[int] = None,
    month: Optional[int] = None,
    db: Session = Depends(get_db)
):
    # Note: This endpoint now returns static location data only
    # For time-varying data (ratings, beds, etc.), use the filtering endpoint
    query = db.query(LocationModel)
    
    if location_region:
        query = query.filter(LocationModel.location_region == location_region)
    if location_local_authority:
        query = query.filter(LocationModel.location_local_authority == location_local_authority)
    if location_type_sector:
        query = query.filter(LocationModel.location_type_sector == location_type_sector)
    
    # For time-varying filters, we need to join with period data
    if is_care_home is not None or latest_overall_rating:
        # Get the latest period or specific period
        period_query = db.query(DataPeriod)
        if year and month:
            period_query = period_query.filter(DataPeriod.year == year, DataPeriod.month == month)
        else:
            period_query = period_query.order_by(DataPeriod.year.desc(), DataPeriod.month.desc())
        
        period = period_query.first()
        if period:
            query = query.join(
                LocationPeriodDataModel,
                LocationModel.location_id == LocationPeriodDataModel.location_id
            ).filter(LocationPeriodDataModel.period_id == period.period_id)
            
            if is_care_home is not None:
                query = query.filter(LocationPeriodDataModel.is_care_home == is_care_home)
            if latest_overall_rating:
                query = query.filter(LocationPeriodDataModel.latest_overall_rating == latest_overall_rating)
    
    locations = query.offset(skip).limit(limit).all()
    return locations


@router.get("/{location_id}", response_model=Location)
def get_location(
    location_id: str,
    db: Session = Depends(get_db)
):
    location = db.query(LocationModel).filter(LocationModel.location_id == location_id).first()
    if not location:
        raise HTTPException(status_code=404, detail="Location not found")
    return location


# Removed /original/{original_id} endpoint since location_id is now the string ID


@router.get("/search/nearby")
def get_nearby_locations(
    latitude: float = Query(..., ge=-90, le=90),
    longitude: float = Query(..., ge=-180, le=180),
    radius_km: float = Query(10, ge=1, le=100),
    limit: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db)
):
    # Simple bounding box search (for more precise distance, use PostGIS)
    lat_offset = radius_km / 111.0  # Rough km to degree conversion
    lng_offset = radius_km / (111.0 * abs(latitude))
    
    locations = db.query(LocationModel).filter(
        LocationModel.latitude.between(latitude - lat_offset, latitude + lat_offset),
        LocationModel.longitude.between(longitude - lng_offset, longitude + lng_offset)
    ).limit(limit).all()
    
    return locations


@router.post("/", response_model=Location)
def create_location(location: LocationCreate, db: Session = Depends(get_db)):
    db_location = LocationModel(**location.dict())
    db.add(db_location)
    db.commit()
    db.refresh(db_location)
    return db_location