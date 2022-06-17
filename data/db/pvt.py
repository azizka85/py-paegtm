from pypika import Query

import globals
from globals import pvt

async def list():
  async with globals.db.acquire() as conn:
    data = await conn.fetch(
      str(
        Query.select(
          pvt.id,
          pvt.geo_id,
          pvt.geo_name_ru,
          pvt.geo_name_short_ru,
          pvt.geo_field_code,
          pvt.density_oil,
          pvt.density_water,
          pvt.date
        ).from_(
          pvt
        )
      )
    )

    return [dict(row) for row in data]
