from pypika import Query, functions as fn

import globals
from globals import gtm_summary, gtm_summary_tmp, gtm_summary_list_count_per_page

async def list(well_id: int):
  async with globals.db.acquire() as conn:
    data = await conn.fetch(
      str(
        Query.select(gtm_summary.star).from_(
          gtm_summary
        ).where(
          gtm_summary.well == well_id
        ).orderby(
          gtm_summary.calc_dbeg
        )
      )
    )

    return [dict(row) for row in data]

async def count():
  async with globals.db.acquire() as conn:
    return await conn.fetchval(
      str(
        Query.select(
          fn.Count(gtm_summary.star)
        ).from_(
          gtm_summary
        )
      )
    )


async def success_list(page_index: int):
  async with globals.db.acquire() as conn:    
    data = await conn.fetch(
      str(
        Query.select(
          gtm_summary.star
        ).from_(
          gtm_summary
        ).orderby(
          gtm_summary.uwi
        ).offset(
          (page_index - 1) * gtm_summary_list_count_per_page
        ).limit(
          gtm_summary_list_count_per_page
        ).where(
          gtm_summary.success_oil >= 0.9
        )
      )
    )

    return [dict(row) for row in data]

async def success_count():
  async with globals.db.acquire() as conn:
    return await conn.fetchval(
      str(
        Query.select(
          fn.Count(gtm_summary.star)
        ).from_(
          gtm_summary
        ).where(
          gtm_summary.success_oil >= 0.9
        )
      )
    )


async def unsuccess_count():
  async with globals.db.acquire() as conn:
    return await conn.fetchval(
      str(
        Query.select(
          fn.Count(gtm_summary.star)
        ).from_(
          gtm_summary
        ).where(
          gtm_summary.success_oil < 0.9
        )
      )
    )


async def unsuccess_wc_list(page_index: int):
  async with globals.db.acquire() as conn:    
    data = await conn.fetch(
      str(
        Query.select(
          gtm_summary.star
        ).from_(
          gtm_summary
        ).orderby(
          gtm_summary.uwi
        ).offset(
          (page_index - 1) * gtm_summary_list_count_per_page
        ).limit(
          gtm_summary_list_count_per_page
        ).where(
          (gtm_summary.success_oil < 0.9) & 
          (gtm_summary.total_add_liquid_rate > gtm_summary.total_add_oil_rate/0.8)
        )
      )
    )

    return [dict(row) for row in data]

async def unsuccess_wc_count():
  async with globals.db.acquire() as conn:
    return await conn.fetchval(
      str(
        Query.select(
          fn.Count(gtm_summary.star)
        ).from_(
          gtm_summary
        ).where(
          (gtm_summary.success_oil < 0.9) & 
          (gtm_summary.total_add_liquid_rate > gtm_summary.total_add_oil_rate/0.8)
        )
      )
    )  


async def unsuccess_liquid_list(page_index: int):
  async with globals.db.acquire() as conn:    
    data = await conn.fetch(
      str(
        Query.select(
          gtm_summary.star
        ).from_(
          gtm_summary
        ).orderby(
          gtm_summary.uwi
        ).offset(
          (page_index - 1) * gtm_summary_list_count_per_page
        ).limit(
          gtm_summary_list_count_per_page
        ).where(
          (gtm_summary.success_oil < 0.9) & 
          (gtm_summary.total_add_liquid_rate < 0.9*gtm_summary.plan_liquid_rate)
        )
      )
    )

    return [dict(row) for row in data]

async def unsuccess_liquid_count():
  async with globals.db.acquire() as conn:
    return await conn.fetchval(
      str(
        Query.select(
          fn.Count(gtm_summary.star)
        ).from_(
          gtm_summary
        ).where(
          (gtm_summary.success_oil < 0.9) & 
          (gtm_summary.total_add_liquid_rate < 0.9*gtm_summary.plan_liquid_rate)
        )
      )
    )  


async def unsuccess_other_list(page_index: int):
  async with globals.db.acquire() as conn:    
    data = await conn.fetch(
      str(
        Query.select(
          gtm_summary.star
        ).from_(
          gtm_summary
        ).orderby(
          gtm_summary.uwi
        ).offset(
          (page_index - 1) * gtm_summary_list_count_per_page
        ).limit(
          gtm_summary_list_count_per_page
        ).where(
          (gtm_summary.success_oil < 0.9) & 
          (gtm_summary.total_add_liquid_rate < gtm_summary.total_add_oil_rate/0.8) &
          (gtm_summary.total_add_liquid_rate > 0.9*gtm_summary.plan_liquid_rate)
        )
      )
    )

    return [dict(row) for row in data]    

async def unsuccess_other_count():
  async with globals.db.acquire() as conn:
    return await conn.fetchval(
      str(
        Query.select(
          fn.Count(gtm_summary.star)
        ).from_(
          gtm_summary
        ).where(
          (gtm_summary.success_oil < 0.9) & 
          (gtm_summary.total_add_liquid_rate < gtm_summary.total_add_oil_rate/0.8) &
          (gtm_summary.total_add_liquid_rate > 0.9*gtm_summary.plan_liquid_rate)
        )
      )
    )      
    

async def add(
  summary: list
):
  async with globals.db.acquire() as conn:
    async with conn.transaction():
      for item in summary:
        await conn.execute(
          str(
            Query.into(
              gtm_summary_tmp
            ).columns(
              gtm_summary_tmp.well,
              gtm_summary_tmp.uwi,
              gtm_summary_tmp.gtm,
              gtm_summary_tmp.gtm_type,
              gtm_summary_tmp.gtm_kind,
              gtm_summary_tmp.gtm_kind_name_short_ru,
              gtm_summary_tmp.gtm_dbeg,
              gtm_summary_tmp.gtm_dend,
              gtm_summary_tmp.gtm_decline_rates_id,
              gtm_summary_tmp.decrease_base_rate,
              gtm_summary_tmp.decrease_gtm_rate,
              gtm_summary_tmp.gtm_factor_analysis_id,
              gtm_summary_tmp.carried_out_gtms_id,
              gtm_summary_tmp.pvt,
              gtm_summary_tmp.pc_ce,
              gtm_summary_tmp.decline_base_rate,
              gtm_summary_tmp.decline_gtm_rate,
              gtm_summary_tmp.total_add_liquid_rate,
              gtm_summary_tmp.total_add_oil_rate,
              gtm_summary_tmp.plan_liquid_rate,
              gtm_summary_tmp.plan_oil_rate,
              gtm_summary_tmp.total_work_after_gtm,
              gtm_summary_tmp.density_oil,
              gtm_summary_tmp.density_water,
              gtm_summary_tmp.pc,
              gtm_summary_tmp.ce,
              gtm_summary_tmp.dev_liquid_rate,
              gtm_summary_tmp.dev_oil_rate,
              gtm_summary_tmp.success_oil,
              gtm_summary_tmp.calc_dbeg,
              gtm_summary_tmp.calc_dend,
              gtm_summary_tmp.org_id,
              gtm_summary_tmp.geo_id
            ).insert(
              item['well'],
              item['uwi'],
              item['gtm'],
              item['gtm_type'],
              item['gtm_kind'],
              item['gtm_kind_name_short_ru'],
              item['gtm_dbeg'],
              item['gtm_dend'],
              item['gtm_decline_rates_id'],
              item['decrease_base_rate'],
              item['decrease_gtm_rate'],
              item['gtm_factor_analysis_id'],
              item['carried_out_gtms_id'],
              item['pvt'],
              item['pc_ce'],
              item['decline_base_rate'],
              item['decline_gtm_rate'],
              item['total_add_liquid_rate'],
              item['total_add_oil_rate'],
              item['plan_liquid_rate'],
              item['plan_oil_rate'],
              item['total_work_after_gtm'],
              item['density_oil'],
              item['density_water'],
              item['pc'],
              item['ce'],
              item['dev_liquid_rate'],
              item['dev_oil_rate'],
              item['success_oil'],
              item['calc_dbeg'],
              item['calc_dend'],
              item['org_id'],
              item['geo_id']
            )            
          )
        )      

async def update_table():
  async with globals.db.acquire() as conn:   
    async with conn.transaction():
      await conn.execute(
        str(
          Query.from_(gtm_summary).delete()
        )
      ) 

      await conn.execute(
        str(
          Query.into(
            gtm_summary
          ).from_(
            gtm_summary_tmp
          ).select(
            gtm_summary_tmp.star
          )
        )
      )      

async def clear_tmp_table():
  async with globals.db.acquire() as conn:
    await conn.execute(
      str(
        Query.from_(gtm_summary_tmp).delete()
      )
    )  
