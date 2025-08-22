from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from app.core.database import get_db
from app.models.provider import Provider as ProviderModel
from app.schemas.provider import Provider, ProviderCreate

router = APIRouter()


@router.get("/", response_model=List[Provider])
def get_providers(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    region: Optional[str] = None,
    type_sector: Optional[str] = None,
    db: Session = Depends(get_db)
):
    query = db.query(ProviderModel)
    
    if region:
        query = query.filter(ProviderModel.region == region)
    if type_sector:
        query = query.filter(ProviderModel.type_sector == type_sector)
    
    providers = query.offset(skip).limit(limit).all()
    return providers


@router.get("/{provider_id}", response_model=Provider)
def get_provider(provider_id: str, db: Session = Depends(get_db)):
    provider = db.query(ProviderModel).filter(ProviderModel.provider_id == provider_id).first()
    if not provider:
        raise HTTPException(status_code=404, detail="Provider not found")
    return provider


@router.post("/", response_model=Provider)
def create_provider(provider: ProviderCreate, db: Session = Depends(get_db)):
    db_provider = ProviderModel(**provider.dict())
    db.add(db_provider)
    db.commit()
    db.refresh(db_provider)
    return db_provider