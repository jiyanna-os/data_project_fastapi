from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from app.core.config import settings
from app.core.database import get_db, engine, Base
from app.api import locations, providers, brands, data_import, location_data_reconstruction, data_filtering

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