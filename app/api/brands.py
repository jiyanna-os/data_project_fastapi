from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from app.core.database import get_db
from app.models.brand import Brand as BrandModel
from app.schemas.brand import Brand, BrandCreate

router = APIRouter()


@router.get("/", response_model=List[Brand])
def get_brands(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db)
):
    brands = db.query(BrandModel).offset(skip).limit(limit).all()
    return brands


@router.get("/{brand_id}", response_model=Brand)
def get_brand(brand_id: str, db: Session = Depends(get_db)):
    brand = db.query(BrandModel).filter(BrandModel.brand_id == brand_id).first()
    if not brand:
        raise HTTPException(status_code=404, detail="Brand not found")
    return brand


@router.post("/", response_model=Brand)
def create_brand(brand: BrandCreate, db: Session = Depends(get_db)):
    db_brand = BrandModel(**brand.dict())
    db.add(db_brand)
    db.commit()
    db.refresh(db_brand)
    return db_brand