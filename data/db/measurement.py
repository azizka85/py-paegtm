from pypika import Query

import globals
from globals import meas_liq, meas_water_cut

async def meas_liq_list(well_id: int):
  async with globals.db.acquire() as conn:    
    data = await conn.fetch(
      str(
        Query.select(
          meas_liq.id,
          meas_liq.dbeg,
          meas_liq.dend,
          meas_liq.liquid
        ).from_(
          meas_liq
        ).where(
          (meas_liq.well == well_id) &
          (meas_liq.dend >= meas_liq.dbeg) &
          (meas_liq.liquid.notnull())
        ).orderby(
          meas_liq.dbeg
        )        
      )
    )

    return [dict(row) for row in data]

async def meas_water_cut_list(well_id: int):
  async with globals.db.acquire() as conn:    
    data = await conn.fetch(
      str(
        Query.select(
          meas_water_cut.id,
          meas_water_cut.dbeg,
          meas_water_cut.dend,
          meas_water_cut.water_cut
        ).from_(
          meas_water_cut
        ).where(
          (meas_water_cut.well == well_id) &
          (meas_water_cut.dend >= meas_water_cut.dbeg) &
          (meas_water_cut.water_cut.notnull())
        ).orderby(
          meas_water_cut.dbeg
        )        
      )
    )

    return [dict(row) for row in data]  
