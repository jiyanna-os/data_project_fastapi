from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Query
from sqlalchemy.orm import Session
from typing import Dict, Any
from pathlib import Path
import logging
from app.core.database import get_db
from app.utils.data_import import CQCDataImporter
from app.utils.parquet_converter import ParquetConverter
from app.utils.import_status import import_tracker

router = APIRouter()
logger = logging.getLogger(__name__)


def import_data_background(excel_path: str, db: Session, filter_care_homes: bool = None, year: int = None, month: int = None):
    """Background task to import data with Parquet optimization"""
    try:
        # Convert to Parquet files for faster processing
        data_folder = Path(excel_path).parent
        converter = ParquetConverter()
        conversion_result = converter.convert_ods_to_parquet(excel_path, str(data_folder))
        
        main_parquet = conversion_result["main_parquet"]
        dual_parquet = conversion_result["dual_parquet"]
        
        # Import from optimized Parquet files
        importer = CQCDataImporter(db)
        stats = importer.import_from_parquet(main_parquet, dual_parquet, filter_care_homes, year, month)
        logger.info(f"Parquet import completed: {stats}")
    except Exception as e:
        logger.error(f"Background import failed: {str(e)}")


@router.post("/import-by-filename")
def import_by_filename(
    background_tasks: BackgroundTasks,
    filename: str = Query(..., description="Filename in format mm_yyyy.ods or mm_yyyy.xlsx (e.g., '08_2025.ods', '06_2025.xlsx')"),
    run_in_background: bool = Query(False, description="Run import in background"),
    filter_care_homes: bool = Query(None, description="Filter: True=care homes only, False=non-care homes only, None=all"),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Import CQC data by filename from Data folder with Parquet optimization.
    
    This endpoint automatically converts ODS/XLSX files to Parquet format for dramatically faster processing,
    then uses an optimized dual registration lookup system for improved performance.
    
    Args:
        filename: Filename in format mm_yyyy.ods or mm_yyyy.xlsx (e.g., "08_2025.ods", "06_2025.xlsx")
        run_in_background: If True, run import as background task
        filter_care_homes: If True, import only care homes; if False, import only non-care homes; if None, import all
        
    Returns:
        Import statistics and status, including Parquet conversion metrics
        
    Performance: ~10-20x faster than direct ODS import (45 minutes → 2-5 minutes)
        
    Examples:
        - filename="08_2025.ods" (converts to Parquet, then imports August 2025 data)
        - filename="06_2025.xlsx" (converts to Parquet, then imports June 2025 data)
        - filename="01_2024.ods" (converts to Parquet, then imports January 2024 data)
    """
    import re
    import os
    
    # Parse month and year from filename (support both .ods and .xlsx)
    pattern = r'^(\d{2})_(\d{4})\.(ods|xlsx)$'
    match = re.match(pattern, filename)
    
    if not match:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid filename format. Expected format: mm_yyyy.ods or mm_yyyy.xlsx (e.g., 08_2025.ods, 06_2025.xlsx). Got: {filename}"
        )
    
    month = int(match.group(1))
    year = int(match.group(2))
    
    # Validate month
    if not (1 <= month <= 12):
        raise HTTPException(status_code=400, detail=f"Month must be between 01 and 12. Got: {month:02d}")
    
    # Validate year
    if year < 2000 or year > 2030:
        raise HTTPException(status_code=400, detail=f"Year must be between 2000 and 2030. Got: {year}")
    
    # Construct full file path
    data_folder = Path("Data")
    file_path = data_folder / filename
    
    # Validate file exists
    if not file_path.exists():
        available_files = []
        if data_folder.exists():
            available_files = list(data_folder.glob('*.ods')) + list(data_folder.glob('*.xlsx'))
        raise HTTPException(
            status_code=404, 
            detail=f"File not found: {file_path}. Available files: {available_files if available_files else 'Data folder not found'}"
        )
    
    if run_in_background:
        background_tasks.add_task(import_data_background, str(file_path), db, filter_care_homes, year, month)
        filter_msg = ""
        if filter_care_homes is True:
            filter_msg = " (care homes only)"
        elif filter_care_homes is False:
            filter_msg = " (non-care homes only)"
        
        return {
            "message": f"Optimized Parquet import started in background{filter_msg}",
            "filename": filename,
            "file_path": str(file_path),
            "year": year,
            "month": month,
            "filter_care_homes": filter_care_homes,
            "status": "running",
            "optimization": "ODS will be converted to Parquet files for faster processing"
        }
    
    # Run import synchronously with Parquet optimization
    try:
        file_size_mb = file_path.stat().st_size / (1024*1024)
        logger.info(f"🚀 Starting optimized import process for {filename}")
        logger.info(f"📁 Source file: {file_path} ({file_size_mb:.1f} MB)")
        
        # Start status tracking
        import_id = import_tracker.start_import(filename, file_size_mb)
        logger.info(f"📊 Import tracking ID: {import_id}")
        
        # Convert ODS/XLSX to Parquet files for faster processing
        logger.info("🔄 Phase 1: Converting to Parquet files for optimized processing...")
        import_tracker.update_phase("parquet_conversion", "Converting ODS file to Parquet format", 10)
        converter = ParquetConverter()
        
        try:
            conversion_result = converter.convert_ods_to_parquet(str(file_path), str(data_folder))
        except Exception as conversion_error:
            error_msg = f"Parquet conversion failed: {str(conversion_error)}"
            logger.error(error_msg)
            
            # Update status tracker
            import_tracker.fail_import(str(conversion_error))
            
            # Check if it's a timeout-related error
            if "timeout" in str(conversion_error).lower():
                return {
                    "message": "Import failed due to file processing timeout",
                    "error": "File is too large or complex for processing. Consider using a smaller file or contact support.",
                    "filename": filename,
                    "file_size_mb": round(file_path.stat().st_size / (1024*1024), 1),
                    "status": "timeout_error",
                    "import_id": import_id,
                    "suggestions": [
                        "Try using a smaller monthly data file",
                        "Ensure the file is not corrupted", 
                        "Consider breaking large files into smaller chunks",
                        "Contact support if this persists"
                    ]
                }
            else:
                raise conversion_error
        
        main_parquet = conversion_result["main_parquet"]
        dual_parquet = conversion_result["dual_parquet"]
        
        import_tracker.complete_phase("parquet_conversion")
        logger.info("✅ Conversion phase completed successfully!")
        logger.info(f"📊 Parquet files created:")
        logger.info(f"   - Main data: {main_parquet}")
        logger.info(f"   - Dual registrations: {dual_parquet}")
        
        # Import from optimized Parquet files
        logger.info("🚀 Phase 2: Starting optimized import from Parquet files...")
        import_tracker.update_phase("data_import", "Importing data from Parquet files", 50)
        importer = CQCDataImporter(db)
        stats = importer.import_from_parquet(main_parquet, dual_parquet, filter_care_homes, year, month)
        
        import_tracker.complete_phase("data_import")
        import_tracker.complete_import(stats)
        
        logger.info("🎉 Import process completed successfully!")
        logger.info(f"📈 Performance summary: {stats.get('records_processed', 0)} records in {stats.get('import_time_seconds', 0):.1f}s")
        
        filter_msg = ""
        if filter_care_homes is True:
            filter_msg = " (care homes only)"
        elif filter_care_homes is False:
            filter_msg = " (non-care homes only)"
        
        return {
            "message": f"Optimized Parquet import completed{filter_msg}",
            "filename": filename,
            "file_path": str(file_path),
            "year": year,
            "month": month,
            "filter_care_homes": filter_care_homes,
            "status": "completed",
            "import_id": import_id,
            "parquet_files": {
                "main_data": main_parquet,
                "dual_registrations": dual_parquet
            },
            "conversion_stats": conversion_result["stats"],
            "import_statistics": stats
        }
        
    except Exception as e:
        logger.error(f"Import failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Import failed: {str(e)}")


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
    [DEPRECATED] Import CQC data from Excel/ODS file to database.
    Use /import-by-filename endpoint for automatic filename parsing.
    
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



@router.get("/available-files")
def list_available_files() -> Dict[str, Any]:
    """
    List all available data files in the Data folder.
    
    Returns:
        List of available files with parsed month/year information
    """
    import re
    
    try:
        data_folder = Path("Data")
        
        if not data_folder.exists():
            return {
                "status": "error",
                "message": "Data folder not found",
                "files": []
            }
        
        files_info = []
        pattern = r'^(\d{2})_(\d{4})\.(ods|xlsx)$'
        
        # Get all ODS and XLSX files in the Data folder
        for file_path in list(data_folder.glob("*.ods")) + list(data_folder.glob("*.xlsx")):
            filename = file_path.name
            match = re.match(pattern, filename)
            
            if match:
                month = int(match.group(1))
                year = int(match.group(2))
                month_names = ["", "January", "February", "March", "April", "May", "June",
                              "July", "August", "September", "October", "November", "December"]
                
                files_info.append({
                    "filename": filename,
                    "month": month,
                    "year": year,
                    "month_name": month_names[month] if 1 <= month <= 12 else "Invalid",
                    "display_name": f"{month_names[month] if 1 <= month <= 12 else 'Invalid'} {year}",
                    "file_size": file_path.stat().st_size,
                    "valid_format": True
                })
            else:
                files_info.append({
                    "filename": filename,
                    "month": None,
                    "year": None,
                    "month_name": None,
                    "display_name": f"{filename} (invalid format)",
                    "file_size": file_path.stat().st_size,
                    "valid_format": False
                })
        
        # Sort by year and month (most recent first)
        files_info.sort(key=lambda x: (x.get('year') or 0, x.get('month') or 0), reverse=True)
        
        return {
            "status": "success",
            "data_folder": str(data_folder),
            "total_files": len(files_info),
            "valid_files": len([f for f in files_info if f['valid_format']]),
            "files": files_info
        }
        
    except Exception as e:
        logger.error(f"Failed to list available files: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to list available files: {str(e)}")


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


@router.post("/convert-ods-to-parquet")
def convert_ods_to_parquet(
    filename: str = Query(..., description="ODS filename in Data folder (e.g., '06_2025.ods')"),
    background_tasks: BackgroundTasks = None,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Convert ODS file to Parquet format for faster import processing.
    
    Args:
        filename: ODS filename in Data folder
        
    Returns:
        Conversion results and Parquet file paths
    """
    try:
        # Validate filename format
        import re
        pattern = r'^(\d{2})_(\d{4})\.(ods)$'
        match = re.match(pattern, filename)
        
        if not match:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid filename format. Expected format: mm_yyyy.ods (e.g., 06_2025.ods). Got: {filename}"
            )
        
        # Check if ODS file exists
        data_folder = Path("Data")
        ods_file_path = data_folder / filename
        
        if not ods_file_path.exists():
            available_files = list(data_folder.glob("*.ods")) if data_folder.exists() else []
            raise HTTPException(
                status_code=404,
                detail=f"ODS file not found: {ods_file_path}. Available ODS files: {available_files}"
            )
        
        # Convert ODS to Parquet
        converter = ParquetConverter()
        result = converter.convert_ods_to_parquet(str(ods_file_path), str(data_folder))
        
        return {
            "status": "success",
            "message": "ODS file converted to Parquet format successfully",
            "source_file": str(ods_file_path),
            "parquet_files": {
                "main_data": result["main_parquet"],
                "dual_registrations": result["dual_parquet"]
            },
            "conversion_stats": result["stats"]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"ODS to Parquet conversion failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Conversion failed: {str(e)}")


def import_parquet_background(main_parquet: str, dual_parquet: str, db: Session, filter_care_homes: bool = None, year: int = None, month: int = None):
    """Background task to import data from Parquet files"""
    try:
        importer = CQCDataImporter(db)
        stats = importer.import_from_parquet(main_parquet, dual_parquet, filter_care_homes, year, month)
        logger.info(f"Parquet import completed: {stats}")
    except Exception as e:
        logger.error(f"Background Parquet import failed: {str(e)}")


@router.post("/import-parquet-by-filename")
def import_parquet_by_filename(
    background_tasks: BackgroundTasks,
    filename: str = Query(..., description="Base filename without extension (e.g., '06_2025' for '06_2025_main.parquet')"),
    run_in_background: bool = Query(False, description="Run import in background"),
    filter_care_homes: bool = Query(None, description="Filter: True=care homes only, False=non-care homes only, None=all"),
    auto_convert: bool = Query(True, description="Automatically convert ODS to Parquet if Parquet files don't exist"),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Import CQC data from Parquet files for optimized performance.
    
    Args:
        filename: Base filename without extension (e.g., "06_2025" for "06_2025_main.parquet")
        run_in_background: If True, run import as background task
        filter_care_homes: If True, import only care homes; if False, import only non-care homes; if None, import all
        auto_convert: If True, automatically convert from ODS if Parquet files don't exist
        
    Returns:
        Import statistics and status
        
    Examples:
        - filename="06_2025" (imports from 06_2025_main.parquet and 06_2025_dual.parquet)
        - filename="08_2025" (imports from 08_2025_main.parquet and 08_2025_dual.parquet)
    """
    import re
    
    try:
        # Parse month and year from filename
        pattern = r'^(\d{2})_(\d{4})$'
        match = re.match(pattern, filename)
        
        if not match:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid filename format. Expected format: mm_yyyy (e.g., 06_2025). Got: {filename}"
            )
        
        month = int(match.group(1))
        year = int(match.group(2))
        
        # Validate month and year
        if not (1 <= month <= 12):
            raise HTTPException(status_code=400, detail=f"Month must be between 01 and 12. Got: {month:02d}")
        
        if year < 2000 or year > 2030:
            raise HTTPException(status_code=400, detail=f"Year must be between 2000 and 2030. Got: {year}")
        
        # Construct Parquet file paths
        data_folder = Path("Data")
        main_parquet = data_folder / f"{filename}_main.parquet"
        dual_parquet = data_folder / f"{filename}_dual.parquet"
        
        # Check if Parquet files exist
        if not (main_parquet.exists() and dual_parquet.exists()):
            if auto_convert:
                # Try to find and convert the corresponding ODS file
                ods_file = data_folder / f"{filename}.ods"
                
                if ods_file.exists():
                    logger.info(f"Parquet files not found, auto-converting from ODS: {ods_file}")
                    converter = ParquetConverter()
                    conversion_result = converter.convert_ods_to_parquet(str(ods_file), str(data_folder))
                    
                    # Update paths to the newly created Parquet files
                    main_parquet = Path(conversion_result["main_parquet"])
                    dual_parquet = Path(conversion_result["dual_parquet"])
                else:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Neither Parquet files nor ODS source file found. Missing: {main_parquet}, {dual_parquet}, {ods_file}"
                    )
            else:
                raise HTTPException(
                    status_code=404,
                    detail=f"Parquet files not found: {main_parquet}, {dual_parquet}. Set auto_convert=true to convert from ODS."
                )
        
        # Validate Parquet files
        converter = ParquetConverter()
        if not converter.validate_parquet_files(str(main_parquet), str(dual_parquet)):
            raise HTTPException(status_code=400, detail="Parquet file validation failed")
        
        if run_in_background:
            background_tasks.add_task(
                import_parquet_background, 
                str(main_parquet), 
                str(dual_parquet), 
                db, 
                filter_care_homes, 
                year, 
                month
            )
            
            filter_msg = ""
            if filter_care_homes is True:
                filter_msg = " (care homes only)"
            elif filter_care_homes is False:
                filter_msg = " (non-care homes only)"
            
            return {
                "message": f"Parquet import started in background{filter_msg}",
                "filename": filename,
                "main_parquet": str(main_parquet),
                "dual_parquet": str(dual_parquet),
                "year": year,
                "month": month,
                "filter_care_homes": filter_care_homes,
                "status": "running"
            }
        
        # Run import synchronously
        importer = CQCDataImporter(db)
        stats = importer.import_from_parquet(str(main_parquet), str(dual_parquet), filter_care_homes, year, month)
        
        filter_msg = ""
        if filter_care_homes is True:
            filter_msg = " (care homes only)"
        elif filter_care_homes is False:
            filter_msg = " (non-care homes only)"
        
        return {
            "message": f"Parquet import completed{filter_msg}",
            "filename": filename,
            "main_parquet": str(main_parquet),
            "dual_parquet": str(dual_parquet),
            "year": year,
            "month": month,
            "filter_care_homes": filter_care_homes,
            "status": "completed",
            "statistics": stats
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Parquet import failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Import failed: {str(e)}")


@router.get("/import-status")
async def get_import_status(import_id: str = None):
    """
    Get the status of a running or completed import operation.
    
    Args:
        import_id: Optional specific import ID to check. If not provided, returns most recent import status.
        
    Returns:
        Current import status with progress information
        
    Example:
        GET /api/v1/data/import-status?import_id=import_1724798400
    """
    try:
        status = import_tracker.get_status(import_id)
        
        if not status:
            return {
                "message": "No import status found",
                "import_id": import_id,
                "status": "not_found"
            }
        
        return {
            "message": "Import status retrieved successfully", 
            "import_status": status
        }
        
    except Exception as e:
        logger.error(f"Failed to get import status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get import status: {str(e)}")