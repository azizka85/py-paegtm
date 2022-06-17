from pypika import Query

import globals
from globals import org, well_org

async def by_well(well_id: int):
  async with globals.db.acquire() as conn:
    row = await conn.fetchrow(
      str(
        Query.select(
          org.id,
          org.parent,
          org.name_ru,
          org.name_short_ru,
          org.org_type,
          org.dbeg,
          org.dend
        ).from_(
          well_org
        ).left_outer_join(
          org
        ).on(
          org.id == well_org.org
        ).where(
          well_org.well == well_id
        ).limit(1)
      )
    )

    return dict(row) if row else None

async def get(org_id: int):
  async with globals.db.acquire() as conn:
    row = await conn.fetchrow(
      str(
        Query.select(
          org.id,
          org.parent,
          org.name_ru,
          org.name_short_ru,
          org.org_type,
          org.dbeg,
          org.dend
        ).from_(
          org
        ).where(
          org.id == org_id
        ).limit(1)
      )
    )

    return dict(row) if row else None
