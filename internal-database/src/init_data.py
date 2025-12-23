from utils.pull import (pull_company_infos,
                        process_input_data)
from utils.utils import (import_market_status_table,
                         import_markets_table,
                         import_exchanges_table,
                         delete_schema)
from utils.init import init_db
from pprint import pprint
from utils.utils import _init_env, import_companies_table


_init_env()
delete_schema()
init_db()
import_markets_table()
import_market_status_table()
import_exchanges_table()