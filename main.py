from json import decoder
import os

import asyncpg

from fastapi import FastAPI

from dotenv import load_dotenv

from routes import gtm_summary, \
    org, \
    geo, \
    gtm, prod_month_report, \
    well, \
    well_status, \
    measurement, \
    pvt, \
    pc_ce, \
    carried_out_gtms, \
    gtm_factors_analysis, \
    gtm_decline_rates

import helpers.datetime

import globals

load_dotenv()

app = FastAPI()

app.include_router(org.router)
app.include_router(geo.router)
app.include_router(gtm.router)
app.include_router(well.router)
app.include_router(well_status.router)
app.include_router(measurement.router)
app.include_router(pvt.router)
app.include_router(pc_ce.router)
app.include_router(gtm_decline_rates.router)
app.include_router(gtm_summary.router)
app.include_router(prod_month_report.router)
app.include_router(carried_out_gtms.router)
app.include_router(gtm_factors_analysis.router)

@app.on_event('startup')
async def startup():
  globals.db = await asyncpg.create_pool(
    os.getenv('DATABASE_URL')
  )

  """ async with globals.db.acquire() as conn:
    await conn.set_type_codec(
      'timestamptz',
      schema='pg_catalog',
      encoder=helpers.datetime.encode,
      decoder=helpers.datetime.decode,
      format='tuple'
    ) """

@app.on_event('shutdown')
async def shutdown():
  if globals.db and not globals.db._closed:
    await globals.db.close()
