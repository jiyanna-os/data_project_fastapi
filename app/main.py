from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
import logging
import sys
from app.core.config import settings
from app.core.database import get_db, engine, Base
from app.api import locations, providers, brands, data_import, location_data_reconstruction, data_filtering

# Configure logging with better formatting
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('cqc_api.log', mode='a', encoding='utf-8')
    ]
)

# Set specific loggers to INFO level to ensure our import logs show up
logging.getLogger('app.utils.parquet_converter').setLevel(logging.INFO)
logging.getLogger('app.utils.data_import').setLevel(logging.INFO)
logging.getLogger('app.api.data_import').setLevel(logging.INFO)

# Also set uvicorn loggers to INFO to see server logs
logging.getLogger('uvicorn.access').setLevel(logging.INFO)
logging.getLogger('uvicorn.error').setLevel(logging.INFO)

# Get the root logger to confirm setup
logger = logging.getLogger(__name__)
logger.info("🚀 CQC API starting with enhanced logging enabled")

# Create tables
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title=settings.app_name,
    description="API for CQC Healthcare Data",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(locations.router, prefix="/api/v1/locations", tags=["locations"])
app.include_router(providers.router, prefix="/api/v1/providers", tags=["providers"])
app.include_router(brands.router, prefix="/api/v1/brands", tags=["brands"])
app.include_router(data_import.router, prefix="/api/v1/data", tags=["data-import"])
app.include_router(location_data_reconstruction.router, prefix="/api/v1/reconstruct", tags=["data-reconstruction"])
app.include_router(data_filtering.router, prefix="/api/v1/filter", tags=["data-filtering"])


@app.get("/")
async def root():
    return {"message": "CQC Data API", "version": "1.0.0"}


@app.get("/health")
async def health_check(db: Session = Depends(get_db)):
    return {"status": "healthy", "database": "connected"}


@app.get("/test-logging")
async def test_logging():
    """Test endpoint to verify logging is working"""
    logger.info("🧪 Test logging endpoint called")
    logger.info("📊 This is an INFO level message")
    logger.warning("⚠️  This is a WARNING level message")
    logger.error("❌ This is an ERROR level message")
    
    # Test import loggers specifically
    import_logger = logging.getLogger('app.utils.data_import')
    import_logger.info("🔄 Testing data import logger")
    
    converter_logger = logging.getLogger('app.utils.parquet_converter')
    converter_logger.info("📁 Testing parquet converter logger")
    
    return {
        "message": "Logging test completed",
        "check_console": "Look at your console/terminal where you started the server",
        "check_file": "Also check cqc_api.log file in the project root"
    }