import data.org as org

async def root(well_id: int): 
  org_data = await org.by_well(well_id)

  if not org_data:
    return None

  parent_id = org_data['parent']

  while parent_id:
    org_data = await org.get(parent_id)

    if not org_data:
      return None

    parent_id = org_data['parent']

  return org_data
  
