from datetime import datetime
import pytz

from asyncpg import Pool

from pypika import Schema

from mako.lookup import TemplateLookup

tz_info = pytz.timezone('Asia/Almaty')

# ub_date = datetime(3333, 1, 1, 0, 0, 0, 0, tzinfo=tz_info)

db: Pool | None = None

templates_lookup = TemplateLookup(directories=['./templates'], output_encoding='utf-8')

dict_schema = Schema('dict')
prod_schema = Schema('prod')
paegtm_schema = Schema('paegtm')

org = dict_schema.org
well_org = prod_schema.well_org

geo = dict_schema.geo

well = dict_schema.well
well_type = dict_schema.well_type

well_status = prod_schema.well_status
well_status_type = dict_schema.well_status_type

gtm = prod_schema.gtm
gtm_type = dict_schema.gtm_type
gtm_kind = dict_schema.gtm_kind

meas_liq = prod_schema.meas_liq
meas_water_cut = prod_schema.meas_water_cut

prod_month_report = paegtm_schema.prod_month_report
prod_month_report_tmp = paegtm_schema.prod_month_report_tmp

gtm_summary_list_count_per_page = 100
gtm_summary = paegtm_schema.gtm_summary
gtm_summary_tmp = paegtm_schema.gtm_summary_tmp

pc_ce = paegtm_schema.pc_ce

pvt = paegtm_schema.pvt

gtm_decline_rates = paegtm_schema.gtm_decline_rates

carried_out_gtms = paegtm_schema.carried_out_gtms
gtm_factors_analysis = paegtm_schema.gtm_factors_analysis
