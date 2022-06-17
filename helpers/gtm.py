def map_gtm_kind(kind_ru: str):
  match kind_ru:    
    case 'ПНП':
      return 'base_fund'
    case 'ПЗП (ОПЗ)':
      return 'opz'
    case 'ППД':
      return 'base_fund'
    case 'ВНС':
      return 'vns'
    case 'ГС':
      return 'gs'
    case 'ВПП':
      return 'base_fund'
    case 'БС':
      return 'base_fund'
    case 'ЗБС,ЗБГС':
      return 'zbs'
    case 'Углубления':
      return 'ugl'
    case 'ГРП':
      return 'grp'
    case 'ВБД':
      return 'vbd'
    case 'ВПС':
      return 'base_fund'
    case 'ПВЛГ':
      return 'pvlg'
    case 'ИДН':
      return 'base_fund'
    case 'РИР':
      return 'rir'
    case 'ПВР':
      return 'pvr'
    case 'ЛАР':
      return 'base_fund'
    case 'ЗБГС':
      return 'zbgs'
    case 'Внедрение УЭЦН':
      return 'base_fund'
    case 'ВНС_ГРП':
      return 'vns_grp'
    case 'ГС_ГРП':
      return 'gs_grp'
    case '---':
      return 'base_fund'
    case 'Скин ГРП':
      return 'base_fund'
    case _:
      return 'base_fund'

