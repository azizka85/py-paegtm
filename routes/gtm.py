from fastapi import APIRouter

import data.gtm as gtm

router = APIRouter()

@router.get('/gtm/list/{well_id}')
async def list(
  well_id: int
):
  return await gtm.list(well_id)

@router.get('/gtm/last/{well_id}')
async def last(
  well_id: int
):
  return await gtm.last(well_id)
