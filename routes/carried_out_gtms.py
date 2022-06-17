from fastapi import APIRouter

import data.carried_out_gtms as carried_out_gtms

import helpers.carried_out_gtms

router = APIRouter()

@router.get('/carried-out-gtms/list/{well_id}')
async def list(
  well_id: int
):
  return helpers.carried_out_gtms.group_by_gtm_date(
    await carried_out_gtms.list(well_id)
  )
