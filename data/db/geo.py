from pypika import Query

import globals
from globals import geo

async def get(field_code: str):
  async with globals.db.acquire() as conn:
    row = await conn.fetchrow(
      str(
        Query.select(
          geo.id,
          geo.name_ru,
          geo.name_short_ru,
          geo.geo_type,
        ).from_(
          geo
        ).where(
          geo.field_code == field_code
        ).limit(1)
      )
    )

    return dict(row) if row else None
