from fastapi import APIRouter

import data.geo as geo

router = APIRouter()

@router.get('/geo/{field_code}')
async def get(
  field_code: str
):
  return await geo.get(field_code)
