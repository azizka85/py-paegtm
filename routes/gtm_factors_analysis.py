from fastapi import APIRouter

import data.gtm_factors_analysis as gtm_factors_analysis

import helpers.gtm_factors_analysis

router = APIRouter()

@router.get('/gtm-factors-analysis/list/{well_id}')
async def list(
  well_id: int
):
  return helpers.gtm_factors_analysis.group_by_gtm_date(
    await gtm_factors_analysis.list(well_id)
  )
