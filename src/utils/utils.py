from config.nse_holidays import NSE_HOLIDAYS_2024
from datetime import datetime, time
import pytz

def is_nse_holiday(date_str):
    # date_str: 'YYYY-MM-DD'
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    if dt.weekday() >= 5:
        return True
    if date_str in NSE_HOLIDAYS_2024:
        return True
    return False

def is_market_hours():
    """Check if current time is within NSE market hours (9:15 AM - 3:30 PM IST)"""
    ist_tz = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist_tz)
    
    # Check if it's a weekday
    if now.weekday() >= 5:  # Saturday = 5, Sunday = 6
        return False
    
    # Check if it's a holiday
    today_str = now.strftime('%Y-%m-%d')
    if is_nse_holiday(today_str):
        return False
    
    # Check market hours (9:15 AM - 3:30 PM IST)
    market_start = time(9, 15)  # 9:15 AM
    market_end = time(15, 30)   # 3:30 PM
    
    current_time = now.time()
    
    return market_start <= current_time <= market_end

def get_market_status():
    """Get detailed market status information"""
    ist_tz = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist_tz)
    
    status = {
        'current_time': now.strftime('%Y-%m-%d %H:%M:%S IST'),
        'is_weekend': now.weekday() >= 5,
        'is_holiday': is_nse_holiday(now.strftime('%Y-%m-%d')),
        'is_market_hours': is_market_hours(),
        'market_open': '09:15:00',
        'market_close': '15:30:00'
    }
    
    return status 