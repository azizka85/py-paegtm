from fastapi import APIRouter
from data.db.measurement import meas_water_cut_list

import data.gtm as gtm
import data.well_status as well_status
import data.measurement as measurement
import data.gtm_summary as summary_dal
import data.prod_month_report as prod_month_report
import data.well as well
import data.geo as geo
import data.pvt as pvt
import data.pc_ce as pc_ce
import data.carried_out_gtms as cog
import data.gtm_factors_analysis as gfa
import data.gtm_decline_rates as gdr

import helpers.org
import helpers.pc_ce
import helpers.pvt
import helpers.gtm_decline_rates
import helpers.gtm_factors_analysis
import helpers.carried_out_gtms
import helpers.measurement

router = APIRouter()

@router.get('/measurement/upload-calculation')
async def upload_calculation():
  num = 1
  cnt_not_geo = 0
  cnt_not_org = 0
  cnt_not_pvt = 0
  cnt_not_gdr = 0
  cnt_not_pc_ce = 0
  cnt_not_gtm = 0
  cnt_not_ws = 0
  cnt_not_meas_liq = 0
  cnt_not_meas_wc = 0

  well_id = 0

  pvt_data = await pvt.list()

  print('Found:', len(pvt_data), 'pvt data')

  pvt_dates, pvt_data = helpers.pvt.group_by_date_geo(
    pvt_data
  )

  pc_ce_data = await pc_ce.list()

  print('Found:', len(pc_ce_data), 'pc_ce data')

  pc_ce_dates, pc_ce_data = helpers.pc_ce.group_by_date_geo(
    pc_ce_data
  )

  gdr_data = await gdr.list()

  print('Found:', len(gdr_data), 'gtm_decline_rates data')

  gdr_dates, gdr_data = helpers.gtm_decline_rates.group_by_date_geo(
    gdr_data
  )

  await summary_dal.clear_tmp_table()    
  await prod_month_report.clear_tmp_table()

  while well_id != None:
    well_data = await well.next(well_id)

    if not well_data:
      break

    well_id = well_data['id']
    uwi = well_data['uwi']

    if not well_id or not uwi or len(uwi) <= 4:
      print("Can't process data for well: ", (uwi, well_id), ', well_id or uwi not defined or not corrected')
      cnt_not_geo += 1
      continue

    field_code = uwi[:3]

    geo_data = await geo.get(field_code)

    if not geo_data:
      print("Can't process data for well:", (uwi, well_id), ', geo for field_code:', field_code, ' not defined')
      cnt_not_geo += 1
      continue

    org_data = await helpers.org.root(well_id)

    if not org_data:
      print("Can't process data for well:", (uwi, well_id), ', org not defined')
      cnt_not_org += 1
      continue

    if not helpers.pvt.get_by_geo(pvt_dates, pvt_data, geo_data['id']):
      print("For well:", (uwi, well_id), ', pvt for field_code:', field_code, ' not defined')
      cnt_not_pvt += 1
      continue

    if not helpers.gtm_decline_rates.get_by_geo(gdr_dates, gdr_data, geo_data['id']):
      print("For well:", (uwi, well_id), ', gtm_decline_rates for field_code:', field_code, ' not defined')
      cnt_not_gdr += 1
      continue

    if not helpers.pc_ce.get_by_geo(pc_ce_dates, pc_ce_data, geo_data['id']):
      print("For well:", (uwi, well_id), ', pc_ce for field_code:', field_code, ' not defined')
      cnt_not_pc_ce += 1
      continue

    gtm_data = await gtm.list(well_id)

    if len(gtm_data) == 0:
      print("Can't process data for well:", (uwi, well_id), ', gtm list is empty')
      cnt_not_gtm += 1
      continue

    ws_data = await well_status.work_list(well_id)

    if len(ws_data) == 0:
      print("Can't process data for well:", (uwi, well_id), ', well_status work list is empty')
      cnt_not_ws += 1
      continue

    meas_liq_data = await measurement.meas_liq_list(well_id)

    if len(meas_liq_data) == 0:
      print("Can't process data for well:", (uwi, well_id), ', liquid measurement list is empty')
      cnt_not_meas_liq += 1
      continue

    meas_wc_data = await measurement.meas_water_cut_list(well_id)

    if len(meas_wc_data) == 0:
      print("Can't process data for well:", (uwi, well_id), ', water cut measurement list is empty')
      cnt_not_meas_wc += 1
      continue

    cog_dates, cog_data = helpers.carried_out_gtms.group_by_gtm_date(
      await cog.list(well_id)
    )

    gfa_dates, gfa_data = helpers.gtm_factors_analysis.group_by_gtm_date(
      await gfa.list(well_id)
    )

    print('Found ml:', len(meas_liq_data))
    print('Found wc:', len(meas_wc_data))
    print('Found ws:', len(meas_wc_data))

    print('Start processing well: ', (uwi, well_id), ', num:', num)

    result = helpers.measurement.calculate(
      well_data, 
      geo_data,
      org_data,
      pvt_dates,
      pvt_data,
      pc_ce_dates,
      pc_ce_data,
      gdr_dates,
      gdr_data,
      gtm_data,
      ws_data, 
      meas_liq_data,
      meas_wc_data,
      cog_dates,
      cog_data,
      gfa_dates,
      gfa_data
    )

    if not result['not_correct_ws']:
      prod_month = result['prod_month']
      summary = result['summary']

      print('prod_month: Number of records for well_id = ', well_id, ' is ', len(prod_month))
      print('summary: Number of records for well_id = ', well_id, ' is ', len(summary))

      await summary_dal.add(summary)
      await prod_month_report.add(prod_month)

      print('Finish processing well: ', (uwi, well_id))
      print('')
    else:
      print("Can't process data for well:", (uwi, well_id), 'well status dend is not correct')        

    num += 1

  await summary_dal.update_table()
  await prod_month_report.update_table()
    
  
  return {
    'processed': num-1,
    'cnt_not_geo': cnt_not_geo,
    'cnt_not_org': cnt_not_org,
    'cnt_not_pvt': cnt_not_pvt,
    'cnt_not_gdr': cnt_not_gdr,
    'cnt_not_pc_ce': cnt_not_pc_ce,
    'cnt_not_gtm': cnt_not_gtm,
    'cnt_not_ws': cnt_not_ws,
    'cnt_not_meas_liq': cnt_not_meas_liq,
    'cnt_not_meas_wc': cnt_not_meas_wc
  }

@router.get('/measurement/list/liquid/{well_id}')
async def list(
  well_id: int
):
  return await measurement.meas_liq_list(well_id)

@router.get('/measurement/list/water-cut/{well_id}')
async def list(
  well_id: int
):
  return await measurement.meas_water_cut_list(well_id)

