# CQC Data API

FastAPI application for CQC Healthcare data with SQLAlchemy models and PostgreSQL database.

## Features

- **FastAPI** with automatic OpenAPI documentation
- **SQLAlchemy** ORM with proper relationships
- **PostgreSQL** database with optimized schema
- **Pydantic** schemas for request/response validation
- **Alembic** for database migrations
- **CORS** enabled for frontend integration

## Database Schema

- **Brands**: Healthcare provider brands
- **Providers**: Healthcare providers with geographic and regulatory info
- **Locations**: Individual healthcare locations
- **Regulated Activities**: Junction table for location activities
- **Service Types**: Junction table for location service types  
- **Service User Bands**: Junction table for target demographics
- **Monthly Snapshots**: Time-series data for historical analysis

## Setup

1. Install dependencies:
```bash
cd cqc_api
pip install -r requirements.txt
```

2. Configure database:
```bash
cp .env.example .env
# Edit .env with your database credentials
```

3. Run the application:
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## API Endpoints

- **GET** `/api/v1/locations/` - List locations with filters
- **GET** `/api/v1/locations/{location_id}` - Get specific location
- **GET** `/api/v1/locations/search/nearby` - Find nearby locations
- **GET** `/api/v1/providers/` - List providers with filters
- **GET** `/api/v1/providers/{provider_id}` - Get specific provider
- **GET** `/api/v1/brands/` - List brands
- **GET** `/api/v1/brands/{brand_id}` - Get specific brand

## Documentation

- Interactive API docs: http://localhost:8000/docs
- ReDoc documentation: http://localhost:8000/redoc