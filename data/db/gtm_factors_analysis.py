from pypika import Query

import globals
from globals import gtm_factors_analysis

async def list(well_id: int):
  async with globals.db.acquire() as conn:      
    data = await conn.fetch(
      str(
        Query.select(
          gtm_factors_analysis.id,
          gtm_factors_analysis.well,
          gtm_factors_analysis.formation_index_before_gtm,
          gtm_factors_analysis.formation_index_after_gtm,
          gtm_factors_analysis.q_l_before_gtm,
          gtm_factors_analysis.q_o_before_gtm,
          gtm_factors_analysis.wct_before_gtm,
          gtm_factors_analysis.gtm,
          gtm_factors_analysis.gtm_kind_id,
          gtm_factors_analysis.gtm_date,
          gtm_factors_analysis.q_l_plan,
          gtm_factors_analysis.q_o_plan,
          gtm_factors_analysis.wct_plan,
          gtm_factors_analysis.q_l_after_gtm,
          gtm_factors_analysis.q_o_after_gtm,
          gtm_factors_analysis.wct_after_gtm,
          gtm_factors_analysis.q_l_deviation,
          gtm_factors_analysis.q_o_deviation,
          gtm_factors_analysis.failure_factor,
          gtm_factors_analysis.failure_reason,
          gtm_factors_analysis.status
        ).from_(
          gtm_factors_analysis
        ).orderby(
          gtm_factors_analysis.gtm_date       
        ).where(
          gtm_factors_analysis.well == well_id
        )        
      )
    )

    return [dict(row) for row in data]
