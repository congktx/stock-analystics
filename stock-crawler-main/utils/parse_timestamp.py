from datetime import datetime, timezone

def parse_date_to_timestamp(date: str):    
    dt = datetime.strptime(date, "%Y-%m-%d")   
    dt = dt.replace(tzinfo=timezone.utc)           
    timestamp = dt.timestamp()         
    return timestamp       
