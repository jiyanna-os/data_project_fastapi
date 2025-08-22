from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from typing import Dict, Any
from pathlib import Path
import logging
from app.core.database import get_db
from app.utils.data_import import CQCDataImporter

router = APIRouter()
logger = logging.getLogger(__name__)


def import_data_background(excel_path: str, db: Session, filter_care_homes: bool = None, year: int = None, month: int = None):
    """Background task to import data"""
    try:
        importer = CQCDataImporter(db)
        stats = importer.import_from_excel(excel_path, filter_care_homes, year, month)
        logger.info(f"Import completed: {stats}")
    except Exception as e:
        logger.error(f"Background import failed: {str(e)}")


@router.post("/import-excel")
def import_excel_data(
    background_tasks: BackgroundTasks,
    year: int,
    month: int,
    file_path: str,
    run_in_background: bool = False,
    filter_care_homes: bool = None,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Import CQC data from Excel/ODS file to database.
    
    Args:
        year: Year of the data (e.g., 2025)
        month: Month of the data (1-12)
        file_path: Full path to the Excel (.xlsx) or ODS (.ods) file
        run_in_background: If True, run import as background task
        filter_care_homes: If True, import only care homes; if False, import only non-care homes; if None, import all
        
    Returns:
        Import statistics and status
        
    Examples:
        - file_path="/home/user/data/01AugustLatest.xlsx"
        - file_path="/home/user/data/01 July 2025 HSCA Active Locations.ods"
    """
    # Validate parameters
    if not (1 <= month <= 12):
        raise HTTPException(status_code=400, detail="Month must be between 1 and 12")
    
    if year < 2000 or year > 2030:
        raise HTTPException(status_code=400, detail="Year must be between 2000 and 2030")
    
    # Validate file exists
    file_obj = Path(file_path)
    if not file_obj.exists():
        raise HTTPException(
            status_code=404, 
            detail=f"File not found: {file_path}"
        )
    
    # Validate file extension
    if not file_path.lower().endswith(('.xlsx', '.xls', '.ods')):
        raise HTTPException(
            status_code=400,
            detail="File must be an Excel (.xlsx, .xls) or OpenDocument (.ods) file"
        )
    
    if run_in_background:
        background_tasks.add_task(import_data_background, file_path, db, filter_care_homes, year, month)
        filter_msg = ""
        if filter_care_homes is True:
            filter_msg = " (care homes only)"
        elif filter_care_homes is False:
            filter_msg = " (non-care homes only)"
        
        return {
            "message": f"Data import started in background{filter_msg}",
            "file_path": file_path,
            "year": year,
            "month": month,
            "filter_care_homes": filter_care_homes,
            "status": "running"
        }
    
    # Run import synchronously
    try:
        importer = CQCDataImporter(db)
        stats = importer.import_from_excel(file_path, filter_care_homes, year, month)
        
        filter_msg = ""
        if filter_care_homes is True:
            filter_msg = " (care homes only)"
        elif filter_care_homes is False:
            filter_msg = " (non-care homes only)"
        
        return {
            "message": f"Data import completed{filter_msg}",
            "file_path": file_path,
            "year": year,
            "month": month,
            "filter_care_homes": filter_care_homes,
            "status": "completed",
            "statistics": stats
        }
        
    except Exception as e:
        logger.error(f"Import failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Import failed: {str(e)}")


@router.get("/import-status")
def get_import_status(db: Session = Depends(get_db)) -> Dict[str, Any]:
    """Get current database statistics"""
    from app.models.brand import Brand
    from app.models.provider import Provider
    from app.models.location import Location
    from app.models.regulated_activity import RegulatedActivity
    from app.models.service_type import ServiceType
    from app.models.service_user_band import ServiceUserBand
    
    try:
        stats = {
            "brands": db.query(Brand).count(),
            "providers": db.query(Provider).count(),
            "locations": db.query(Location).count(),
            "regulated_activities": db.query(RegulatedActivity).count(),
            "service_types": db.query(ServiceType).count(),
            "service_user_bands": db.query(ServiceUserBand).count()
        }
        
        return {
            "status": "success",
            "database_statistics": stats
        }
        
    except Exception as e:
        logger.error(f"Failed to get statistics: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get statistics: {str(e)}")


@router.delete("/clear-data")
def clear_all_data(
    confirm: bool = False,
    db: Session = Depends(get_db)
) -> Dict[str, str]:
    """
    Clear all data from database (USE WITH CAUTION!)
    
    Args:
        confirm: Must be True to actually clear data
    """
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail="Must set confirm=True to clear data"
        )
    
    try:
        from app.models.location import Location
        from app.models.provider import Provider
        from app.models.brand import Brand
        from app.models.regulated_activity import RegulatedActivity, LocationRegulatedActivity
        from app.models.service_type import ServiceType, LocationServiceType
        from app.models.service_user_band import ServiceUserBand, LocationServiceUserBand
        from app.models.monthly_snapshot import MonthlySnapshot, LocationSnapshotData
        
        # Delete in order due to foreign key constraints
        db.query(LocationSnapshotData).delete()
        db.query(MonthlySnapshot).delete()
        db.query(LocationServiceUserBand).delete()
        db.query(LocationServiceType).delete()
        db.query(LocationRegulatedActivity).delete()
        db.query(Location).delete()
        db.query(Provider).delete()
        db.query(Brand).delete()
        db.query(ServiceUserBand).delete()
        db.query(ServiceType).delete()
        db.query(RegulatedActivity).delete()
        
        db.commit()
        
        return {"message": "All data cleared successfully"}
        
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to clear data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to clear data: {str(e)}")


@router.post("/recreate-tables")
def recreate_tables(
    confirm: bool = False,
    db: Session = Depends(get_db)
) -> Dict[str, str]:
    """
    Drop and recreate all database tables (USE WITH CAUTION!)
    
    Args:
        confirm: Must be True to actually recreate tables
    """
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail="Must set confirm=True to recreate tables"
        )
    
    try:
        from app.core.database import engine, Base
        
        # Drop all tables
        Base.metadata.drop_all(bind=engine)
        
        # Recreate all tables
        Base.metadata.create_all(bind=engine)
        
        return {"message": "Database tables recreated successfully"}
        
    except Exception as e:
        logger.error(f"Failed to recreate tables: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to recreate tables: {str(e)}")


@router.get("/data-periods")
def get_data_periods(db: Session = Depends(get_db)) -> Dict[str, Any]:
    """Get all data periods with location counts"""
    try:
        from app.models.data_period import DataPeriod
        from app.models.location import Location
        
        periods = db.query(DataPeriod).order_by(DataPeriod.year.desc(), DataPeriod.month.desc()).all()
        
        period_list = []
        for period in periods:
            # Count locations in this period
            location_count = db.query(Location).filter(
                Location.period_id == period.period_id
            ).count()
            
            period_list.append({
                "period_id": period.period_id,
                "year": period.year,
                "month": period.month,
                "month_name": ["", "January", "February", "March", "April", "May", "June",
                              "July", "August", "September", "October", "November", "December"][period.month],
                "file_name": period.file_name,
                "location_count": location_count,
                "created_at": period.created_at.isoformat() if period.created_at else None
            })
        
        return {
            "status": "success",
            "data_periods": period_list
        }
        
    except Exception as e:
        logger.error(f"Failed to get data periods: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get data periods: {str(e)}")



@router.get("/location-history/{location_id}")
def get_location_history(
    location_id: str,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Get historical data for a specific location across all periods"""
    try:
        from app.models.data_period import DataPeriod
        from app.models.location import Location
        
        # Get all instances of this location across different periods
        locations = db.query(Location, DataPeriod).join(
            DataPeriod, Location.period_id == DataPeriod.period_id
        ).filter(
            Location.location_id == location_id
        ).order_by(DataPeriod.year.desc(), DataPeriod.month.desc()).all()
        
        if not locations:
            raise HTTPException(status_code=404, detail="Location not found")
        
        history_list = []
        for location, period in locations:
            history_list.append({
                "year": period.year,
                "month": period.month,
                "month_name": ["", "January", "February", "March", "April", "May", "June",
                              "July", "August", "September", "October", "November", "December"][period.month],
                "file_name": period.file_name,
                "location_name": location.location_name,
                "latest_overall_rating": location.latest_overall_rating,
                "is_dormant": location.is_dormant,
                "is_care_home": location.is_care_home,
                "care_homes_beds": location.care_homes_beds,
                "provider_id": location.provider_id,
                "region": location.region,
                "local_authority": location.local_authority
            })
        
        return {
            "status": "success",
            "location_id": location_id,
            "history": history_list
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get location history: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get location history: {str(e)}")