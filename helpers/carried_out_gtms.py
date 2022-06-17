from datetime import datetime

def group_by_gtm_date(list: list[dict]):
  dates = []
  result = {}
  last_date = None

  for item in list:
    gtm_kind_id = item['gtm_kind_id']
    dd = item['date_start_after_gtm']

    if not last_date or dd > last_date:
      last_date = dd
      dates.append(dd)
    
    if dd not in result:
      result[dd] = {}

    result[dd][gtm_kind_id] = item

  return (dates, result)

def get_by_gtm(dates: list[datetime], data: dict, gtm_kind_id: int):
  for date in reversed(dates):
    if gtm_kind_id in data[date]:
      return data[date][gtm_kind_id]

  return None

def get_by_date_gtm(dates: list[datetime], data: dict, ddt: datetime, gtm_kind_id: int):
  current_cog = None

  ddt = datetime(ddt.year, ddt.month, ddt.day, ddt.hour, ddt.minute, ddt.second, ddt.microsecond)

  for date in dates:
    if gtm_kind_id in data[date]:
      current_cog = data[date][gtm_kind_id]

    if date >= ddt and gtm_kind_id in data[date]:
      break

  return current_cog
