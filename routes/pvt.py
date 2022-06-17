from fastapi import APIRouter

import data.pvt as pvt

import helpers.pvt

router = APIRouter()

@router.get('/pvt/list')
async def get():
  return helpers.pvt.group_by_date_geo(
    await pvt.list()
  )
