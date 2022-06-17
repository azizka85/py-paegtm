from datetime import datetime, date, timedelta
import math

from dateutil.relativedelta import relativedelta

base_date = datetime(2000, 1, 1, 0, 0, 0, 0)
incorrect_date = datetime(1, 1, 1, 0, 0, 0, 0)

def encode(dt: datetime):
  diff = dt - base_date

  return (diff.total_seconds() * 1000000)

def decode(tup):  
  rd = relativedelta(microseconds=tup[0], hours=6)    

  if abs(rd.days) > 500000:
    return incorrect_date

  return base_date + rd

def last_day_month(dt: date):
  new_dt = date(dt.year+1, 1, 1)

  if dt.month < 12:
    new_dt = date(dt.year, dt.month+1, 1)

  return new_dt - timedelta(days=1)

  
