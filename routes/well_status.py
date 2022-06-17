from datetime import date

from fastapi import APIRouter

import data.well_status as well_status

router = APIRouter()

@router.get('/well-status/work-list/{well_id}')
async def work_list(well_id: int):
  return await well_status.work_list(well_id)
