from datetime import datetime, timezone

def parse_date_to_timestamp(date: str):
    dt = datetime.strptime(str, "%Y%m%dT%H%M%S")
    
    dt = dt.replace(tzinfo=timezone.utc)
    
    ts = dt.timestamp()
    
    return ts

def parse_timestamp_to_date(timestamp: int):
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    
    date = dt.strftime("%Y%m%dT%H%M%S")
    
    return date