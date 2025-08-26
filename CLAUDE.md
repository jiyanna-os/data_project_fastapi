# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **FastAPI-based CQC (Care Quality Commission) Healthcare Data API** that provides REST endpoints for querying and filtering UK healthcare location data. The project uses SQLAlchemy ORM with PostgreSQL and handles time-series CQC inspection data from 2015-2025.

## Architecture

### Core Components
- **FastAPI Application**: Main API framework with automatic OpenAPI documentation
- **SQLAlchemy Models**: Database ORM layer with proper relationships and time-series data handling
- **Pydantic Schemas**: Request/response validation and serialization  
- **PostgreSQL Database**: Primary data store with optimized schema for healthcare data
- **Docker Support**: Containerized deployment with health checks

### Key Models and Relationships
- **Location** (app/models/location.py): Core healthcare locations with geographic data
- **Provider** (app/models/provider.py): Healthcare provider organizations
- **LocationPeriodData** (app/models/location_period_data.py): Time-series snapshots of location data
- **Junction Tables**: Many-to-many relationships for activities, service types, and user bands
- **MonthlySnapshot** (app/models/monthly_snapshot.py): Historical data tracking

### API Router Structure
- **Locations**: `/api/v1/locations/` - Location CRUD and search endpoints
- **Providers**: `/api/v1/providers/` - Provider information and filtering
- **Brands**: `/api/v1/brands/` - Healthcare brand data
- **Data Import**: `/api/v1/data/` - Bulk data import from Excel/ODS files
- **Data Filtering**: `/api/v1/filter/` - Advanced filtering with complex query support
- **Data Reconstruction**: `/api/v1/reconstruct/` - Historical data reconstruction

## Common Development Commands

### Running the Application
```bash
# Install dependencies
pip install -r requirements.txt

# Run development server with auto-reload
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# Run with Docker
docker build -t cqc-api .
docker run -p 8000:8000 --env-file .env cqc-api
```

### Database Operations
```bash
# Database migrations with Alembic
alembic upgrade head
alembic revision --autogenerate -m "description"

# PostgreSQL connection required - configure in .env file
```

### Testing and Development
```bash
# Run tests (pytest available in requirements)
pytest

# API documentation available at:
# http://localhost:8000/docs (Swagger UI)
# http://localhost:8000/redoc (ReDoc)
```

## Data Import Architecture

The project handles **monthly CQC data files** with automatic filename-based import:
- **File Format**: `mm_yyyy.ods` (e.g., `08_2025.ods` for August 2025)
- **Current Data**: Located in `Data/` directory (01_2025.ods to 08_2025.ods)
- **Historical Range**: 2015-2025 data supported

### Key Features
- **Automatic Date Extraction**: Month and year extracted from filename (`mm_yyyy.ods`)
- **Standardized Date Parsing**: Comprehensive date format support with 25+ date patterns
- **File Discovery**: Automatic file location in `Data/` folder by filename
- **Date Validation**: Dates are null only when truly empty; handles multiple input formats

### Import Endpoints
- `POST /api/v1/data/import-by-filename` - Import by filename (recommended)
- `GET /api/v1/data/available-files` - List available data files
- `POST /api/v1/data/import-excel` - Legacy full-path import (deprecated)

### Complete Data Reconstruction
The import system now creates comprehensive records including:
- **LocationActivityFlags**: All Y/N boolean flags for regulated activities, service types, and user bands
- **Junction Tables**: Backward-compatible relationships for activities, services, and user bands
- **Time-series Data**: LocationPeriodData for ratings, beds, manager info per period
- **Static Data**: Location and Provider information that doesn't change over time

## Advanced Filtering System

The `/api/v1/filter/filter-data` endpoint provides sophisticated filtering capabilities:

### Filter Types
- **Simple Parameters**: Direct URL parameters for common filters
- **Boolean Service Flags**: Care home types, service categories, user demographics
- **Geographic Filters**: Region, city, latitude/longitude ranges
- **Numeric Ranges**: Bed counts, date ranges
- **JSON Filters**: Complex conditions with multiple operators

### Filter Architecture
- **FilterCondition**: Pydantic model for individual filter rules
- **Available Columns**: Comprehensive mapping of filterable database columns
- **Operators**: eq, contains, starts_with, gt, gte, lt, lte, in, not_in
- **Logic**: AND/OR combination of multiple conditions

## Configuration

### Environment Variables (.env required)
```
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password  
POSTGRES_DB=your_database
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
```

### Database Schema Highlights
- **Time-series design**: Monthly snapshots with proper temporal relationships
- **Geographic data**: Latitude/longitude for mapping capabilities
- **Regulatory data**: CQC ratings, inspection dates, compliance status
- **Service classification**: Activity flags and service user bands for detailed categorization

### Date Columns and Formats
The system handles **3 main date columns** with comprehensive format support:
1. `location_hsca_start_date` (Location model) - When location started HSCA registration
2. `publication_date` (LocationPeriodData model) - When rating/inspection was published
3. `hsca_start_date` (Provider model) - When provider started HSCA registration

**Date Parsing Features:**
- 25+ supported date formats (ISO, UK DD/MM/YYYY, US MM/DD/YYYY, Excel serial dates)
- Automatic pandas fallback parsing with day-first preference (UK format)
- Excel serial date conversion with leap year bug handling
- Null dates only when truly empty (not '-', '*', or other placeholders)

## Key Files for Understanding

- **app/main.py**: Application entry point and router configuration
- **app/core/config.py**: Settings and database URL construction
- **app/api/data_filtering.py**: Advanced filtering implementation with 80+ filterable columns
- **FILTERING_EXAMPLES.md**: Comprehensive API usage examples
- **app/models/**: SQLAlchemy models defining the data relationships