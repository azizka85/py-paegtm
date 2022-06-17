from pypika import Query

import globals
from globals import pc_ce

async def list():
  async with globals.db.acquire() as conn:
    data = await conn.fetch(
      str(
        Query.select(
          pc_ce.id,
          pc_ce.geo_id,
          pc_ce.geo_name_ru,
          pc_ce.geo_name_short_ru,
          pc_ce.geo_field_code,
          pc_ce.pc,
          pc_ce.ce,
          pc_ce.date
        ).from_(
          pc_ce
        ).orderby(
          pc_ce.date
        )
      )
    )

    return [dict(row) for row in data]
