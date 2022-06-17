from pypika import Query

import globals
from globals import gtm_decline_rates

async def list():
  async with globals.db.acquire() as conn:      
    data = await conn.fetch(
      str(
        Query.select(
          gtm_decline_rates.id,
          gtm_decline_rates.org_id,
          gtm_decline_rates.org_name,
          gtm_decline_rates.org_name_short,
          gtm_decline_rates.geo_id,
          gtm_decline_rates.oilfield,
          gtm_decline_rates.date,
          gtm_decline_rates.base_fund,
          gtm_decline_rates.vns,
          gtm_decline_rates.vns_grp,
          gtm_decline_rates.gs,
          gtm_decline_rates.gs_grp,
          gtm_decline_rates.zbs,
          gtm_decline_rates.zbgs,
          gtm_decline_rates.ugl,
          gtm_decline_rates.grp,
          gtm_decline_rates.vbd,
          gtm_decline_rates.pvlg,
          gtm_decline_rates.rir,
          gtm_decline_rates.pvr,
          gtm_decline_rates.opz
        ).from_(
          gtm_decline_rates
        ).orderby(
          gtm_decline_rates.date       
        )        
      )
    )

    return [dict(row) for row in data]
