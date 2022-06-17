from pypika import Query, Order

import globals
from globals import prod_month_report, prod_month_report_tmp

async def add(
  prod_month: list
):
  async with globals.db.acquire() as conn:
    async with conn.transaction():
      for item in prod_month:
        await conn.execute(
          str(
            Query.into(
              prod_month_report_tmp
            ).columns(
              prod_month_report_tmp.date,
              prod_month_report_tmp.well,
              prod_month_report_tmp.uwi,
              prod_month_report_tmp.gtm,
              prod_month_report_tmp.gtm_type,
              prod_month_report_tmp.gtm_kind,
              prod_month_report_tmp.gtm_kind_name_short_ru,
              prod_month_report_tmp.gtm_dbeg,
              prod_month_report_tmp.gtm_dend,
              prod_month_report_tmp.gtm_decline_rates_id,
              prod_month_report_tmp.decrease_base_rate,
              prod_month_report_tmp.decrease_gtm_rate,
              prod_month_report_tmp.pvt,
              prod_month_report_tmp.pc_ce,
              prod_month_report_tmp.decline_base_rate,
              prod_month_report_tmp.decline_gtm_rate,              
              prod_month_report_tmp.density_oil,
              prod_month_report_tmp.density_water,
              prod_month_report_tmp.pc,
              prod_month_report_tmp.ce,
              prod_month_report_tmp.work,
              prod_month_report_tmp.liquid,
              prod_month_report_tmp.oil,
              prod_month_report_tmp.base_liquid,
              prod_month_report_tmp.base_oil,
              prod_month_report_tmp.add_liquid,
              prod_month_report_tmp.add_oil,
              prod_month_report_tmp.add_liquid_rate,
              prod_month_report_tmp.add_oil_rate,
              prod_month_report_tmp.is_fact,
              prod_month_report_tmp.geo_id,
              prod_month_report_tmp.org_id,
              prod_month_report_tmp.plan_liquid_rate,
              prod_month_report_tmp.plan_oil_rate,
              prod_month_report_tmp.plan_liquid,
              prod_month_report_tmp.plan_oil
            ).insert(
              item['date'],
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
              item['pvt'],
              item['pc_ce'],
              item['decline_base_rate'],
              item['decline_gtm_rate'],
              item['density_oil'],
              item['density_water'],
              item['pc'],
              item['ce'],
              item['work'],
              item['liquid'],
              item['oil'],
              item['base_liquid'],
              item['base_oil'],
              item['add_liquid'],
              item['add_oil'],
              item['add_liquid_rate'],
              item['add_oil_rate'],
              item['is_fact'],
              item['geo_id'],
              item['org_id'],
              item['plan_liquid_rate'],
              item['plan_oil_rate'],
              item['plan_liquid'],
              item['plan_oil']
            )            
          )
        ) 

async def update_table():
  async with globals.db.acquire() as conn:    
    async with conn.transaction():
      await conn.execute(
        str(
          Query.from_(prod_month_report).delete()
        )
      )
      
      await conn.execute(
        str(
          Query.into(
            prod_month_report
          ).from_(
            prod_month_report_tmp
          ).select(
            prod_month_report_tmp.star
          )
        )
      )      

async def clear_tmp_table():
  async with globals.db.acquire() as conn:
    await conn.execute(
      str(
        Query.from_(prod_month_report_tmp).delete()
      )
    )   

async def list(well_id: int):
  async with globals.db.acquire() as conn:
    data = await conn.fetch(
      str(
        Query.select(prod_month_report.star).from_(
          prod_month_report
        ).where(
          prod_month_report.well == well_id
        ).orderby(
          prod_month_report.date
        )
      )
    )  

    return [dict(row) for row in data]

async def fact_last_date(well_id: int):
  async with globals.db.acquire() as conn:
    return await conn.fetchval(
      str(
        Query.select(
          prod_month_report.date
        ).from_(
          prod_month_report
        ).where(
          (prod_month_report.well == well_id) &
          (prod_month_report.is_fact == True)
        ).orderby(
          prod_month_report.date, order=Order.desc
        ).limit(1)
      )
    )
