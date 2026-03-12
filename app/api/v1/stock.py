from fastapi import APIRouter

router = APIRouter(prefix="/stocks", tags=["stocks"])

@router.get("/ping")
def ping():
    return {"message": "stocks service alive"}