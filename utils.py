from datetime import datetime
from zoneinfo import ZoneInfo


#logging
CET = ZoneInfo("Europe/Rome")

def log(prefix, message):
    """
    Simple logger function.
    """
    print(f"[{datetime.now(CET).strftime('%Y-%m-%d %H:%M:%S')}][{prefix}] {message}")

