from fastapi import APIRouter

import data.gtm_decline_rates as gtm_decline_rates
import helpers.gtm_decline_rates

router = APIRouter()

@router.get('/gtm-decline-rates/list')
async def list():
  return helpers.gtm_decline_rates.group_by_date_geo(
    await gtm_decline_rates.list()
  )
