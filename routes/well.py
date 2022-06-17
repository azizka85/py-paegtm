from fastapi import APIRouter

import data.well as well

router = APIRouter()

@router.get('/well/{uwi}')
async def get(uwi: str):
  return await well.get(uwi)
