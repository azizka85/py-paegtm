from pypika import Query

import globals
from globals import well, well_type

async def get(uwi: str):
  async with globals.db.acquire() as conn:
    row = await conn.fetchrow(
      str(
        Query.select(
          well.id,
          well.uwi,
          well.project_date,
          well_type.name_ru.as_('well_type_name_ru'),
          well_type.name_short_ru.as_('well_type_name_short_ru')
        ).from_(
          well
        ).left_outer_join(
          well_type
        ).on(
          well_type.id == well.well_type
        ).where(
          well.uwi == uwi
        )
      )
    )

    return dict(row)     

async def next(well_id: int):
  async with globals.db.acquire() as conn:
    row = await conn.fetchrow(
      str(
        Query.select(
          well.id,
          well.uwi
        ).from_(
          well
        ).where(
          well.id > well_id
        ).orderby(
          well.id
        ).limit(1)
      )
    )   

    return dict(row) if row else None
