from utils.config import GlobalConfig
from utils.utils import (_init_env,
                         test,
                         truncate,
                         delete_schema,
                         import_companies_table,
                         import_markets_table,
                         import_market_status_table,
                         import_exchanges_table,
                         import_company_status_table)
from utils.pull import pull_company_infos
from utils.init import init_db

_init_env()
# delete_schema()
# init_db()

pull_company_infos(year=2024, month=2)
# test(sql="SELECT count(*) FROM datasource.companies")
GlobalConfig.CONN.close()