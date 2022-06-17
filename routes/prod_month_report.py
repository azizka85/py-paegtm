from io import BytesIO

import matplotlib.pyplot as plt

from fastapi import APIRouter
from fastapi.responses import HTMLResponse, StreamingResponse

import data.gtm as gtm_dal
import data.well as well
import data.gtm_summary as summary_dal
import data.prod_month_report as prod_month_report

import globals

router = APIRouter()

@router.get('/prod-month-report/plot/liquid/{well_id}')
async def plot_liquid(well_id: int, success: int):
  data = await prod_month_report.list(well_id)

  if data:
    plt.xlabel('Дата')
    plt.ylabel('Добыча, м^3')
    plt.title('Сравнение расчетов добычи жидкости')

    n = len(data)
    m = 8
    k = int(n / m)
    l = n % m

    if k == 0:
      m = l

    dates = [''] * m
    liquid_pts = [0] * m
    liquid_base_pts = [0] * m

    max_liquid = 0

    for i in range(m, 0, -1):
      if k == 0:
        l = i

      max_liquid = max(max_liquid, data[i*k + l - 1]['liquid'], data[i*k + l - 1]['base_liquid'])

      dates[i-1] = data[i*k+l-1]["date"].strftime('%y-%m')
      liquid_pts[i-1] = data[i*k + l - 1]['liquid']
      liquid_base_pts[i-1] = data[i*k+l-1]["base_liquid"]

    plt.xticks(range(0, m), dates)
    
    if success:
      plt.bar(dates, liquid_pts, label='Общая добыча жидкости', color='orange')
      plt.bar(dates, liquid_base_pts, label='Базовая добыча жидкости', color='blue')
    else:      
      plt.bar(dates, liquid_base_pts, label='Базовая добыча жидкости', color='blue')      
      plt.bar(dates, liquid_pts, label='Общая добыча жидкости', color='orange')

    plt.legend()

    plt.gca().set_ylim([0, max_liquid*1.05])

    buf = BytesIO()

    plt.savefig(buf, format='png')

    buf.seek(0)          

    plt.close()

    return StreamingResponse(
      buf,
      media_type='image/png'
    )

  return None

@router.get('/prod-month-report/plot/oil/{well_id}')
async def plot_oil(well_id: int, success: int):
  data = await prod_month_report.list(well_id)  

  if data:
    plt.xlabel('Дата')
    plt.ylabel('Добыча, м^3')
    plt.title('Сравнение расчетов добычи нефти')

    n = len(data)
    m = 8
    k = int(n / m)
    l = n % m

    if k == 0:
      m = l

    dates = [''] * m
    oil_pts = [0] * m
    oil_base_pts = [0] * m

    max_liquid = 0

    for i in range(m, 0, -1):
      if k == 0:
        l = i

      max_liquid = max(max_liquid, data[i*k + l - 1]['liquid'], data[i*k + l - 1]['base_liquid'])

      dates[i-1] = data[i*k+l-1]["date"].strftime('%y-%m')
      oil_pts[i-1] = data[i*k + l - 1]['oil']
      oil_base_pts[i-1] = data[i*k+l-1]["base_oil"]

    plt.xticks(range(0, m), dates)
    
    if success:
      plt.bar(dates, oil_pts, label='Общая добыча нефти', color='orange')
      plt.bar(dates, oil_base_pts, label='Базовая добыча нефти', color='blue')
    else:
      plt.bar(dates, oil_base_pts, label='Базовая добыча нефти', color='blue')
      plt.bar(dates, oil_pts, label='Общая добыча нефти', color='orange')

    plt.legend()
    plt.gca().set_ylim([0, max_liquid*1.05])

    buf = BytesIO()

    plt.savefig(buf, format='png')

    buf.seek(0)          

    plt.close()

    return StreamingResponse(
      buf,
      media_type='image/png'
    )

  return None

@router.get('/prod-month-report/view/{uwi}')
async def list(uwi: str, success: int):
  well_data = await well.get(uwi)
  data = None
  summary = None
  last_date = None

  if well_data:
    data = await prod_month_report.list(well_data['id'])
    summary = await summary_dal.list(well_data['id'])
    last_date = await prod_month_report.fact_last_date(well_data['id'])

  return HTMLResponse(
    content=globals.templates_lookup.get_template('prod-month-report.html').render(          
      summary=summary,
      data=data,
      well=well_data,
      last_date=last_date,
      success=success          
    )
  )
