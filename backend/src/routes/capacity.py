from fastapi import APIRouter,Depends
from src.middlewares.auth import current,admin
from src.schemas.business import CapacityInput
from src.services import capacity
from src.services.audit import log
router=APIRouter()
@router.get('/capacity')
def get(u=Depends(current)):return capacity.status()
@router.put('/capacity')
def put(data:CapacityInput,u=Depends(admin)):
 r=capacity.set_limit(data.limit);log(u.email,'capacity.limit',str(data.limit));return r
