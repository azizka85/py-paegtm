from fastapi import APIRouter

import data.pc_ce as pc_ce

import helpers.pc_ce

router = APIRouter()

@router.get('/pc-ce/list')
async def get():
  return helpers.pc_ce.group_by_date_geo(
    await pc_ce.list()
  )
