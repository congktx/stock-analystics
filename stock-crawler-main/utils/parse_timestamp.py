from datetime import datetime, timezone

def parse_date_to_timestamp(date: str):    
    date_str = "2024-12-01"
    dt = datetime.strptime(date_str, "%Y-%m-%d")   
    dt = dt.replace(tzinfo=timezone.utc)           
    timestamp = dt.timestamp()         
    return timestamp       
