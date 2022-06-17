from datetime import date

def group_by_date_geo(list: list[dict]):
  dates = []
  result = {}
  last_date = None

  for item in list:
    geo_id = item['geo_id']
    dd = item['date']

    if not last_date or dd > last_date:
      last_date = dd
      dates.append(dd)
    
    if dd not in result:
      result[dd] = {}

    result[dd][geo_id] = item

  return (dates, result)

def get_by_geo(dates: list[date], data: dict, geo_id: int):
  for date in reversed(dates):
    if geo_id in data[date]:
      return data[date][geo_id]

  return None

def get_by_date_geo(dates: list[date], data: dict, dd: date, geo_id: int):
  current_pc_ce = None

  for date in dates:
    current_pc_ce = data[date][geo_id]

    if date >= dd and geo_id in data[date]:
      break

  return current_pc_ce
