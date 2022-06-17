from pypika import Query

import globals
from globals import carried_out_gtms

async def list(well_id: int):
  async with globals.db.acquire() as conn:      
    data = await conn.fetch(
      str(
        Query.select(
          carried_out_gtms.id,
          carried_out_gtms.well,
          carried_out_gtms.planned_increase,
          carried_out_gtms.gtm,
          carried_out_gtms.gtm_kind_id,
          carried_out_gtms.date_stop_before_gtm,
          carried_out_gtms.date_start_after_gtm
        ).from_(
          carried_out_gtms
        ).orderby(
          carried_out_gtms.date_start_after_gtm,
          carried_out_gtms.date_stop_before_gtm          
        ).where(
          carried_out_gtms.well == well_id
        )        
      )
    )

    return [dict(row) for row in data]
