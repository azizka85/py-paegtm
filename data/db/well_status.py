from pypika import Query

import globals
from globals import well_status, well_status_type

async def work_list(well_id: int):
  async with globals.db.acquire() as conn:    
    data = await conn.fetch(
      str(
        Query.select(
          well_status.id,
          well_status.dbeg,
          well_status.dend
        ).from_(
          well_status
        ).left_outer_join(
          well_status_type    
        ).on(
          well_status_type.id == well_status.status
        ).where(
          (well_status.well == well_id) &
          (well_status.dend >= well_status.dbeg) &
          (well_status_type.code == 'WRK')
        ).orderby(
          well_status.dbeg
        )        
      )
    )

    return [dict(row) for row in data]


