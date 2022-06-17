import math
from datetime import date, datetime

from dateutil.relativedelta import relativedelta

import helpers.datetime
import helpers.gtm
import helpers.gtm_decline_rates
import helpers.carried_out_gtms
import helpers.gtm_factors_analysis
import helpers.pc_ce
import helpers.pvt

import globals

def get_default_liquid_plan(gtm_kind: str):
  if gtm_kind == 'ВНС' or gtm_kind == 'ВНС_ГРП' or gtm_kind == 'ВБД' or gtm_kind == 'ГС' or gtm_kind == 'ГС_ГРП':
    return 25
  else:
    return 10

def get_default_oil_plan(gtm_kind: str):
  if gtm_kind == 'ВНС' or gtm_kind == 'ВНС_ГРП' or gtm_kind == 'ВБД' or gtm_kind == 'ГС' or gtm_kind == 'ГС_ГРП':
    return 10
  else:
    return 4  

def find_ws_end_calc_date(ws: list[dict]):
  for item in reversed(ws):
    dend = item['dend']
    ub_date = datetime(3333, 1, 1, 0, 0, 0, 0, globals.tz_info)

    if dend < ub_date:
      return dend
  
  return None

def find_end_calc_date_after_last_gtm(
  gtm: list[dict]
):
  gtm_len = len(gtm)

  dend = gtm[gtm_len-1]['dend']
  dend = date(dend.year, dend.month, 1)

  kind = gtm[gtm_len-1]['kind_name_short_ru']

  if kind == 'ВНС' or kind == 'ВНС_ГРП' or kind == 'ВБД' or kind == 'ГС' or kind == 'ГС_ГРП':
    dend += relativedelta(years=5)
  else:
    dend += relativedelta(years=1)

  return dend

def calculate_base_rate(prod_month: list):
  liquid = 0
  oil = 0
  work = 0

  i = len(prod_month)

  months = 0

  while i > 0 and months < 3:
    liquid += prod_month[i-1]['liquid']
    oil += prod_month[i-1]['oil']
    work += prod_month[i-1]['work']

    months += 1

    i -= 1
  
  liquid_rate = 0
  oil_rate = 0

  if work > 0:
    liquid_rate = liquid / work
    oil_rate = oil / work

  return (liquid_rate, oil_rate)

def calculate_summary(
  well: dict,
  geo: dict,
  org: dict,
  current_pvt: dict,
  current_pc_ce: dict,
  current_gtm: dict,
  gtm_kind_key: str,
  decline_rate: dict,
  decrease_rate: float,
  gtm_decrease_rate: float,
  total_work_after_gtm: float,
  total_add_liquid: float,
  total_add_oil: float,
  current_cog,
  current_gfa,
  plan_liquid_rate,
  plan_oil_rate,
  density_oil: float,
  density_water: float,
  pc: float,
  ce: float,
  calc_dbeg: date,
  calc_dend: date
):
  total_add_liquid_rate = 0
  total_add_oil_rate = 0

  if total_work_after_gtm > 0:
    total_add_liquid_rate = total_add_liquid / total_work_after_gtm
    total_add_oil_rate = total_add_oil / total_work_after_gtm

  dev_liquid_rate = total_add_liquid_rate - plan_liquid_rate
  dev_oil_rate = total_add_oil_rate - plan_oil_rate

  success_oil = 0

  if plan_oil_rate > 0:
    success_oil = total_add_oil_rate / plan_oil_rate

  return {
    'well': well['id'],
    'org_id': org['id'],
    'geo_id': geo['id'],
    'uwi': well['uwi'],
    'gtm': current_gtm['id'],
    'gtm_type': current_gtm['gtm_type'],
    'gtm_kind': current_gtm['gtm_kind'],
    'gtm_kind_name_short_ru': current_gtm['kind_name_short_ru'],
    'gtm_dbeg': current_gtm['dbeg'],
    'gtm_dend': current_gtm['dend'],
    'gtm_decline_rates_id': decline_rate['id'],
    'decrease_base_rate': decrease_rate,
    'decrease_gtm_rate': gtm_decrease_rate,
    'gtm_factor_analysis_id': current_gfa['id'] if current_gfa else None,
    'carried_out_gtms_id': current_cog['id'] if current_cog else None,
    'pvt': current_pvt['id'],
    'pc_ce': current_pc_ce['id'],
    'decline_base_rate': decline_rate['base_fund'],
    'decline_gtm_rate': decline_rate[gtm_kind_key],
    'total_add_liquid_rate': total_add_liquid_rate,
    'total_add_oil_rate': total_add_oil_rate,
    'plan_liquid_rate': plan_liquid_rate,
    'plan_oil_rate': plan_oil_rate,
    'total_work_after_gtm': total_work_after_gtm,
    'density_oil': density_oil,
    'density_water': density_water,
    'pc': pc,
    'ce': ce,
    'dev_liquid_rate': dev_liquid_rate,
    'dev_oil_rate': dev_oil_rate,
    'success_oil': success_oil,
    'calc_dbeg': calc_dbeg,
    'calc_dend': calc_dend
  }

def calculate(
  well: dict,
  geo: dict,  
  org: dict,
  pvt_dates: list,
  pvt_data: dict,
  pc_ce_dates: list,
  pc_ce_data: dict,  
  gdr_dates: list,
  gdr_data: dict,  
  gtm: list[dict],
  ws: list[dict],
  meas_liq: list[dict],
  meas_wc: list[dict],
  cog_dates: list,
  cog_data: dict,
  gfa_dates: list,
  gfa_data: dict
):
  not_correct_ws = False

  dbeg = ws[0]['dbeg']
  dend = find_ws_end_calc_date(ws)

  summary = []
  prod_month = []

  if dend == None:
    not_correct_ws = True
    print("Can't process data for well:", well['id'], ', no correct dend in well_status work list')
  else:    
    gtm_decrease_rate = None

    dbeg = date(dbeg.year, dbeg.month, 1)
    dend = helpers.datetime.last_day_month(dend)

    calc_dend = find_end_calc_date_after_last_gtm(gtm)

    if calc_dend < dend:
      calc_dend = dend

    dd = dbeg

    # dbeg_dt = datetime(dbeg.year, dbeg.month, dbeg.day, 0, 0, 0, 0, tzinfo=globals.tz_info)

    ws_len = len(ws)
    gtm_len = len(gtm)
    ml_len = len(meas_liq)
    mwc_len = len(meas_wc)

    gtm_i = 0
    ws_i = 0
    ml_i = 0
    mwc_i = 0

    prev_date_predict = False

    predict_base = False
    predict = False

    base_liquid_rate = 0
    base_oil_rate = 0

    add_liquid_rate = 0
    add_oil_rate = 0

    total_add_liquid = 0
    total_add_oil = 0

    total_work_after_gtm = 0
    total_work_predict = 0

    ce = 0

    current_gtm = None    
    gtm_kind_key = None

    plan_liquid_rate = None
    plan_oil_rate = None

    while dd <= calc_dend:
      work = 0
      liquid = 0
      oil = 0
      base_liquid = 0
      base_oil = 0

      add_liquid = 0
      add_oil = 0

      if dd < dend:
        ddt = datetime(dd.year, dd.month, dd.day, 0, 0, 0, 0, tzinfo=globals.tz_info)

        decline_rate = helpers.gtm_decline_rates.get_by_date_geo(gdr_dates, gdr_data, dd, geo['id'])    
        decrease_rate = math.log(1 - decline_rate['base_fund']/100)/365

        current_pvt = helpers.pvt.get_by_date_geo(pvt_dates, pvt_data, dd, geo['id'])

        density_oil = current_pvt['density_oil']
        density_water = current_pvt['density_water']

        current_pc_ce = helpers.pc_ce.get_by_date_geo(pc_ce_dates, pc_ce_data, dd, geo['id'])

        ce = current_pc_ce['ce']
        pc = current_pc_ce['pc']

        while gtm_i < gtm_len and gtm[gtm_i]['dend'] <= ddt:
          predict_base = True    

          if gtm_decrease_rate:
            decrease_rate = gtm_decrease_rate
          
          if current_gtm != None:
            summary.append(
              calculate_summary(
                well,
                geo,
                org,
                current_pvt,
                current_pc_ce,              
                current_gtm,
                gtm_kind_key,
                decline_rate,
                decrease_rate,
                gtm_decrease_rate,
                total_work_after_gtm,
                total_add_liquid,
                total_add_oil,
                current_cog,
                current_gfa,
                plan_liquid_rate,
                plan_oil_rate,
                density_oil,
                density_water,
                pc, 
                ce,
                gtm_calc_dbeg,
                dd
              )
            )        

          current_gtm = gtm[gtm_i]

          gtm_calc_dbeg = dd

          current_cog = helpers.carried_out_gtms.get_by_date_gtm(cog_dates, cog_data, ddt, current_gtm['gtm_kind'])
          current_gfa = helpers.gtm_factors_analysis.get_by_date_gtm(gfa_dates, gfa_data, ddt, current_gtm['gtm_kind'])
          
          plan_liquid_rate = get_default_liquid_plan(current_gtm['kind_name_short_ru'])
          plan_oil_rate = get_default_oil_plan(current_gtm['kind_name_short_ru'])

          if current_cog:
            plan_oil_rate = current_cog['planned_increase']

          if current_gfa:
            plan_liquid_rate = current_gfa['q_l_plan']          

          gtm_kind_key = helpers.gtm.map_gtm_kind(current_gtm['kind_name_short_ru'])
          gtm_decrease_rate = math.log(1 - decline_rate[gtm_kind_key]/100)/365

          base_liquid_rate, base_oil_rate = calculate_base_rate(prod_month)

          total_work_after_gtm = 0
          total_add_liquid = 0
          total_add_oil = 0          

          gtm_i += 1    

        while ws_i < ws_len and ws[ws_i]['dbeg'] <= ddt + relativedelta(months=1) and ws[ws_i]['dend'] >= ddt:
          while ml_i < ml_len:
            if meas_liq[ml_i]['dbeg'] <= ws[ws_i]['dend'] and meas_liq[ml_i]['dend'] >= ws[ws_i]['dbeg'] and \
              meas_liq[ml_i]['dbeg'] <= ddt + relativedelta(months=1) and meas_liq[ml_i]['dend'] >= ddt:            

              val_dend = min(ddt + relativedelta(months=1), ws[ws_i]['dend'], meas_liq[ml_i]['dend'])
              val_dbeg = max(ddt, ws[ws_i]['dbeg'], meas_liq[ml_i]['dbeg'])            

              if val_dend > val_dbeg:
                total_seconds = (val_dend - val_dbeg).total_seconds()
                liquid_val = meas_liq[ml_i]['liquid'] * total_seconds / 3600 / 24

                wc_seconds = 0

                wc = 0

                while mwc_i < mwc_len and meas_wc[mwc_i]['dbeg'] <= val_dend and meas_wc[mwc_i]['dend'] >= datetime(val_dbeg.year, val_dbeg.month, val_dbeg.day, val_dbeg.hour, val_dbeg.minute, val_dbeg.second, val_dbeg.microsecond):

                  val_dend_utc = datetime(val_dend.year, val_dend.month, val_dend.day, val_dend.hour, val_dend.minute, val_dend.second, val_dend.microsecond)

                  val_mwc_dend = min(meas_wc[mwc_i]['dend'], val_dend_utc)
                  val_mwc_dbeg = max(meas_wc[mwc_i]['dbeg'], val_dbeg)
                  val_mwc_dbeg = datetime(val_mwc_dbeg.year, val_mwc_dbeg.month, val_mwc_dbeg.day, val_mwc_dbeg.hour, val_mwc_dbeg.minute, val_mwc_dbeg.second, val_mwc_dbeg.microsecond)

                  val_seconds = (val_mwc_dend - val_mwc_dbeg).total_seconds()
                  wc_seconds += val_seconds

                  wc += meas_wc[mwc_i]['water_cut'] * val_seconds / total_seconds

                  if meas_wc[mwc_i]['dend'] > val_dend_utc and \
                    (mwc_i == mwc_len-1 or (meas_wc[mwc_i+1]['dbeg'] >= val_dend)):

                    break

                  mwc_i += 1     

                val_mwc_i = mwc_i if mwc_i < mwc_len else mwc_len - 1
                wc += meas_wc[val_mwc_i]['water_cut'] * (total_seconds - wc_seconds) / total_seconds                          

                liquid += liquid_val
                oil += (100 - wc) * liquid_val * density_oil / 100      
                work += total_seconds / 3600 / 24

            if (meas_liq[ml_i]['dend'] > ws[ws_i]['dend'] or meas_liq[ml_i]['dend'] > ddt + relativedelta(months=1)) and \
              (ml_i == ml_len - 1 or (meas_liq[ml_i+1]['dbeg'] >= min(ddt + relativedelta(months=1), ws[ws_i]['dend']))):

              break            

            ml_i += 1

          if ws[ws_i]['dend'] > ddt + relativedelta(months=1) and \
            (ws_i == ws_len - 1 or ws[ws_i+1]['dbeg'] >= ddt + relativedelta(months=1)):

            break

          ws_i += 1
      else:
        predict = True
        
        decrease_pred_rate = gtm_decrease_rate if gtm_decrease_rate else decrease_rate

        new_month = dd + relativedelta(months=1)
        work = ce * (new_month - dd).total_seconds() / 3600 / 24                     
        total_work_predict += work

        if total_work_predict > 0:
          add_liquid = add_liquid_rate * (1 - math.exp(decrease_pred_rate * total_work_predict)) * work / -decrease_pred_rate / total_work_predict
          add_oil = add_oil_rate * density_oil * (1 - math.exp(decrease_pred_rate * total_work_predict)) * work / -decrease_pred_rate / total_work_predict
        else:
          add_liquid = 0
          add_oil = 0            

      if predict_base:
        total_work_after_gtm += work

        if total_work_after_gtm > 0:
          base_liquid = base_liquid_rate * (1 - math.exp(decrease_rate * total_work_after_gtm)) * work / -decrease_rate / total_work_after_gtm
          base_oil = base_oil_rate * density_oil * (1 - math.exp(decrease_rate * total_work_after_gtm)) * work / -decrease_rate / total_work_after_gtm
        else:
          base_liquid = 0
          base_oil = 0
      else:
        base_liquid = liquid
        base_oil = oil

      if predict:
        liquid = base_liquid + add_liquid
        oil = base_oil + add_oil
      else:
        add_liquid = liquid - base_liquid
        add_oil = oil - base_oil

      total_add_liquid += add_liquid
      total_add_oil += add_oil

      add_liquid_rate = 0 if work == 0 else add_liquid / work
      add_oil_rate = 0 if work == 0 else add_oil / work

      if ((predict or dd >= calc_dend) and prev_date_predict == False) and current_gtm:
        prev_date_predict = True

        summary.append(
          calculate_summary(
            well,
            geo,
            org,
            current_pvt,
            current_pc_ce,              
            current_gtm,
            gtm_kind_key,
            decline_rate,
            decrease_rate,
            gtm_decrease_rate,
            total_work_after_gtm,
            total_add_liquid,
            total_add_oil,
            current_cog,
            current_gfa,
            plan_liquid_rate,
            plan_oil_rate,
            density_oil,
            density_water,
            pc, 
            ce,
            gtm_calc_dbeg,
            dd
          )
        )

      prod_month.append({
        'date': dd,
        'well': well['id'],
        'uwi': well['uwi'],
        'gtm': current_gtm['id'] if current_gtm else None,
        'gtm_type': current_gtm['gtm_type'] if current_gtm else None,
        'gtm_kind': current_gtm['gtm_kind'] if current_gtm else None,
        'gtm_kind_name_short_ru': current_gtm['kind_name_short_ru'] if current_gtm else None,
        'gtm_dbeg': current_gtm['dbeg'] if current_gtm else None,
        'gtm_dend': current_gtm['dend'] if current_gtm else None,
        'gtm_decline_rates_id': decline_rate['id'] if decline_rate else None,
        'decrease_base_rate': decrease_rate,
        'decrease_gtm_rate': gtm_decrease_rate,
        'pvt': current_pvt['id'],
        'pc_ce': current_pc_ce['id'],
        'decline_base_rate': decline_rate['base_fund'],
        'decline_gtm_rate': decline_rate[gtm_kind_key] if gtm_kind_key else None,
        'density_oil': density_oil,
        'density_water': density_water,
        'pc': pc,
        'ce': ce,
        'work': work,
        'liquid': liquid,
        'oil': oil,
        'base_liquid': base_liquid,
        'base_oil': base_oil,
        'add_liquid': add_liquid,
        'add_oil': add_oil,
        'add_liquid_rate': add_liquid_rate,
        'add_oil_rate': add_oil_rate,
        'is_fact': not predict,
        'geo_id': geo['id'],
        'org_id': org['id'],
        'plan_liquid_rate': plan_liquid_rate if plan_liquid_rate != None else None,
        'plan_oil_rate': plan_oil_rate  if plan_oil_rate != None else None,
        'plan_liquid': plan_liquid_rate * work if plan_liquid_rate != None else None,
        'plan_oil': plan_oil_rate * work  if plan_oil_rate != None else None
      })

      dd += relativedelta(months=1)

  return {
    'not_correct_ws': not_correct_ws,
    'summary': summary,
    'prod_month': prod_month
  }




  







