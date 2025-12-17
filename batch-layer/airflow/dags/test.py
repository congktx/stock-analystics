from utils import pull_daily_ohlc_data, date_to_timestamp
from datetime import datetime, timedelta

start_date = datetime(year=2025, month=5, day=1)

def pull_data_daily_job():
    global start_date
    from_timestamp = date_to_timestamp(year=start_date.year, 
                                       month=start_date.month,
                                       day=start_date.day,
                                       second=1)
    start_date += timedelta(days=1)
    to_timestamp = date_to_timestamp(year=start_date.year, 
                                     month=start_date.month,
                                     day=start_date.day)
    print(from_timestamp)
    print(to_timestamp)
    pull_daily_ohlc_data(from_timestamp=from_timestamp,
                    to_timestamp=to_timestamp)

pull_data_daily_job()