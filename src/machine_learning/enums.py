from enum import IntEnum, Enum, auto

class BatchSize(IntEnum):
    TICKER = 20
    DATE = 90
    
class FutureDays(Enum):
    WEEK = 7