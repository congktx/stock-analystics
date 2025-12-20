from datetime import datetime, timezone

def round_timestamp(timestamp, round_time=86400):
    timestamp = int(timestamp)
    timestamp_unit_day = timestamp / round_time
    recover_to_unit_second = int(timestamp_unit_day) * round_time
    return recover_to_unit_second

def timestamp_to_date(timestamp):
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d")

def timestamp_to_YYYYMMDDTHHMM(timestamp):
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt.strftime("%Y%m%dT%H%M")