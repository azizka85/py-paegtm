from fastapi import APIRouter
from fastapi.responses import HTMLResponse

import data.gtm_summary as gtm_summary

import globals
from globals import gtm_summary_list_count_per_page

router = APIRouter()

@router.get('/gtm-summary/{well_id}')
async def get(
  well_id: int
):
  return await gtm_summary.list(well_id)

@router.get('/gtm-summary/view/list/success/{page_index}')
async def success_list(page_index: int):
  count = await gtm_summary.success_count()
  pages_count = int(count/gtm_summary_list_count_per_page)

  if count % gtm_summary_list_count_per_page > 0:
    pages_count += 1

  return HTMLResponse(
    content=globals.templates_lookup.get_template('gtm-summary-success.html').render(
      data=await gtm_summary.success_list(page_index),
      count=count,
      pages_count=pages_count
    )
  )

@router.get('/gtm-summary/view/list/unsuccess-wc/{page_index}')
async def unsuccess_wc_list(page_index: int):
  count = await gtm_summary.unsuccess_wc_count()
  pages_count = int(count/gtm_summary_list_count_per_page)

  if count % gtm_summary_list_count_per_page > 0:
    pages_count += 1

  return HTMLResponse(
    content=globals.templates_lookup.get_template('gtm-summary-unsuccess-wc.html').render(
      data=await gtm_summary.unsuccess_wc_list(page_index),
      count=count,
      pages_count=pages_count
    )
  )

@router.get('/gtm-summary/view/list/unsuccess-liquid/{page_index}')
async def unsuccess_liquid_list(page_index: int):
  count = await gtm_summary.unsuccess_liquid_count()
  pages_count = int(count/gtm_summary_list_count_per_page)

  if count % gtm_summary_list_count_per_page > 0:
    pages_count += 1

  return HTMLResponse(
    content=globals.templates_lookup.get_template('gtm-summary-unsuccess-liquid.html').render(
      data=await gtm_summary.unsuccess_liquid_list(page_index),
      count=count,
      pages_count=pages_count
    )
  )

@router.get('/gtm-summary/view/list/unsuccess-other/{page_index}')
async def unsuccess_other_list(page_index: int):
  count = await gtm_summary.unsuccess_other_count()
  pages_count = int(count/gtm_summary_list_count_per_page)

  if count % gtm_summary_list_count_per_page > 0:
    pages_count += 1

  return HTMLResponse(
    content=globals.templates_lookup.get_template('gtm-summary-unsuccess-other.html').render(
      data=await gtm_summary.unsuccess_other_list(page_index),
      count=count,
      pages_count=pages_count
    )
  )

@router.get('/gtm-summary/view/list/unsuccess')
async def list():
  return HTMLResponse(
    content=globals.templates_lookup.get_template('gtm-summary-unsuccess.html').render(
      count=await gtm_summary.unsuccess_count(),
      liquid_count=await gtm_summary.unsuccess_liquid_count(),
      wc_count=await gtm_summary.unsuccess_wc_count(),
      other_count=await gtm_summary.unsuccess_other_count()
    )
  )

@router.get('/gtm-summary/view/list')
async def list():
  return HTMLResponse(
    content=globals.templates_lookup.get_template('gtm-summary.html').render(
      count=await gtm_summary.count(),
      success_count=await gtm_summary.success_count(),
      unsuccess_count=await gtm_summary.unsuccess_count()
    )
  )
