from datetime import datetime
from pypika import Query, Order

import globals
from globals import gtm, gtm_kind, gtm_type

async def list(well_id: int):
  async with globals.db.acquire() as conn:      
    data = await conn.fetch(
      str(
        Query.select(
          gtm.id,
          gtm.gtm_type,
          gtm_type.gtm_kind,
          gtm_type.name_ru.as_('type_name_ru'),
          gtm_type.name_short_ru.as_('type_name_short_ru'),
          gtm_kind.name_ru.as_('kind_name_ru'),
          gtm_kind.name_short_ru.as_('kind_name_short_ru'),
          gtm_kind.code.as_('kind_code'),
          gtm.dbeg,
          gtm.dend
        ).from_(
          gtm
        ).left_outer_join(
          gtm_type
        ).on(
          gtm.gtm_type == gtm_type.id
        ).left_outer_join(
          gtm_kind
        ).on(
          gtm_type.gtm_kind == gtm_kind.id
        ).orderby(
          gtm.dbeg
        ).where(
          (gtm.well == well_id) &
          (gtm.dend >= gtm.dbeg) &
          (gtm.dend <= datetime(3333, 1, 1, 0, 0, 0, 0)) &
          (gtm.dbeg >= datetime(3, 3, 3, 0, 0, 0, 0))
        )        
      )
    )

    return [dict(row) for row in data]

async def last(well_id: int):
  async with globals.db.acquire() as conn:
    row = await conn.fetchrow(
      str(
        Query.select(
          gtm.id,
          gtm.gtm_type,
          gtm_type.gtm_kind,
          gtm_type.name_ru.as_('type_name_ru'),
          gtm_type.name_short_ru.as_('type_name_short_ru'),
          gtm_kind.name_ru.as_('kind_name_ru'),
          gtm_kind.name_short_ru.as_('kind_name_short_ru'),
          gtm_kind.code.as_('kind_code'),
          gtm.dbeg,
          gtm.dend
        ).from_(
          gtm
        ).left_outer_join(
          gtm_type
        ).on(
          gtm.gtm_type == gtm_type.id
        ).left_outer_join(
          gtm_kind
        ).on(
          gtm_type.gtm_kind == gtm_kind.id
        ).orderby(
          gtm.dbeg, order=Order.desc
        ).where(
          (gtm.well == well_id) &
          (gtm.dend >= gtm.dbeg) &
          (gtm.dend <= datetime(3333, 1, 1, 0, 0, 0, 0)) &
          (gtm.dbeg >= datetime(3, 3, 3, 0, 0, 0, 0))
        ).limit(1)
      )
    )

    return dict(row) if row else None
