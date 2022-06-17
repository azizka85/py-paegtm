from fastapi import APIRouter

import data.org as org

import helpers.org

router = APIRouter()

@router.get('/org/{org_id}')
async def get(
  org_id: int
):
  return await org.get(org_id)

@router.get('/org/root/{well_id}')
async def root(
  well_id: int
):
  return await helpers.org.root(well_id)

@router.get('/org/well/{well_id}')
async def by_well(
  well_id: int
):
  return await org.by_well(well_id)
