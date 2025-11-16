import requests
import json
import pandas as pd
import sys
import time
import os
from datetime import datetime, time as dtime
import pytz
from requests.exceptions import RequestException

# Make sure src is on PYTHONPATH for relative imports when run via cron
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.nse_holidays import NSE_HOLIDAYS_2024
from src.nse_scraper import fetch_all_option_chain, format_for_nautilus
from src.utils.utils import is_nse_holiday

# List of fields not required in output while displaying option chain data
EXCLUDE_KEYS = [
    'pchangeinOpenInterest', 'totalBuyQuantity', 'totalSellQuantity',
    'underlyingValue', 'expiryDate', 'underlying', 'identifier', 'pChange'
]

NSE_URL = 'https://www.nseindia.com/api/option-chain-indices?symbol={symbol}'
HEADERS = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/100.0.4896.127 Safari/537.36'
}

OC_COLS = [
    'c_OI', 'c_CHNG_IN_OI', 'c_VOLUME', 'c_IV', 'c_LTP', 'c_CHNG',
    'c_BID_QTY', 'c_BID', 'c_ASK', 'c_ASK_QTY',
    'STRIKE', 'p_BID_QTY', 'p_BID', 'p_ASK', 'p_ASK_QTY',
    'p_CHNG', 'p_LTP', 'p_IV', 'p_VOLUME', 'p_CHNG_IN_OI', 'p_OI'
]

OUTPUT_DIR = os.path.join('data', 'daily')
EXCHANGE = 'NSE'
SYMBOLS = ['NIFTY', 'BANKNIFTY']


# ---------- Helper functions ----------
def set_decimal(x):
    """Restrict decimal value to 2 places, remove trailing zeros."""
    return ('%.2f' % x).rstrip('0').rstrip('.') if isinstance(x, float) else x

def fetch_nse_data(url, headers, timeout=10, retries=3, backoff=2):
    """Fetch data from NSE with retry logic."""
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except (RequestException, json.JSONDecodeError) as e:
            if attempt < retries - 1:
                time.sleep(backoff ** attempt)
            else:
                raise RuntimeError(f"Failed to fetch data from NSE after {retries} attempts: {e}")

def filter_oc_data(expiry_dates, data):
    """Organize option chain data by expiry date."""
    oc_d = {ed: {"CE": [], "PE": []} for ed in expiry_dates}
    for entry in data:
        ed = entry.get('expiryDate')
        if ed in oc_d:
            ce = entry.get('CE')
            pe = entry.get('PE')
            oc_d[ed]["CE"].append(ce if ce and ce.get('expiryDate') == ed else '-')
            oc_d[ed]["PE"].append(pe if pe and pe.get('expiryDate') == ed else '-')
    return oc_d

def create_final_oc_matrix(CE, PE):
    """Format and create final list of Option Chain data similar to NSE website."""
    l_OC = []
    for i in range(len(CE)):
        ce = CE[i]
        pe = PE[i]
        if ce != '-':
            for key in EXCLUDE_KEYS:
                ce.pop(key, None)
        if pe != '-':
            for key in EXCLUDE_KEYS:
                pe.pop(key, None)

    for i in range(len(CE)):
        ce = CE[i]
        pe = PE[i]
        l_CE = []
        l_PE = []
        if ce != '-':
            sp = ce['strikePrice']
            l_CE = [
                ce.get('openInterest', '-'), ce.get('changeinOpenInterest', '-'), ce.get('totalTradedVolume', '-'),
                ce.get('impliedVolatility', '-'), ce.get('lastPrice', '-'), set_decimal(ce.get('change', '-')),
                ce.get('bidQty', '-'), ce.get('bidprice', '-'), ce.get('askPrice', '-'), ce.get('askQty', '-'), sp
            ]
        else:
            sp = pe['strikePrice'] if pe != '-' else '-'
            l_CE = ['-'] * 10 + [sp]
        if pe != '-':
            l_PE = [
                pe.get('bidQty', '-'), pe.get('bidprice', '-'), pe.get('askPrice', '-'), pe.get('askQty', '-'),
                set_decimal(pe.get('change', '-')), pe.get('lastPrice', '-'), pe.get('impliedVolatility', '-'),
                pe.get('totalTradedVolume', '-'), pe.get('changeinOpenInterest', '-'), pe.get('openInterest', '-')
            ]
        else:
            l_PE = ['-'] * 10
        l_OC_t = l_CE + l_PE
        l_OC_t = [x if x != 0 else '-' for x in l_OC_t]
        l_OC.append(l_OC_t)
    return l_OC

def ensure_output_dir():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

def get_today_str():
    return datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d')

# ---------- New market status helpers ----------
def is_market_hours():
    tz = pytz.timezone('Asia/Kolkata')
    now = datetime.now(tz).time()
    start = dtime(9, 15)
    end = dtime(15, 30)
    return start <= now <= end

def get_market_status():
    tz = pytz.timezone('Asia/Kolkata')
    now = datetime.now(tz)
    return {
        'current_time': now.strftime('%Y-%m-%d %H:%M:%S'),
        'market_open': '09:15',
        'market_close': '15:30',
        'is_weekend': now.weekday() >= 5,  # Sat=5, Sun=6
        'is_holiday': is_nse_holiday(now.strftime('%Y-%m-%d'))
    }

# ---------- Main scraper ----------
def main():
    today = datetime.now(pytz.timezone('Asia/Kolkata'))
    today_str = today.strftime('%Y-%m-%d')

    # Get market status
    market_status = get_market_status()
    print(f"Current time: {market_status['current_time']} (IST)")
    print(f"Market hours: {market_status['market_open']} - {market_status['market_close']} IST")

    # Check if market is open
    if not is_market_hours():
        if market_status['is_weekend']:
            print(f"{today_str} is a weekend. Market is closed.")
        elif market_status['is_holiday']:
            print(f"{today_str} is an NSE holiday. Market is closed.")
        else:
            print(f"{today_str} is outside market hours (09:15 - 15:30 IST). Market is closed.")
        return

    ensure_output_dir()
    timestamp = today.strftime('%Y-%m-%d %H:%M:%S')

    for symbol in SYMBOLS:
        print(f"Fetching {symbol} option chain...")
        oc_rows = []
        try:
            all_options = fetch_all_option_chain(symbol)
            for row in format_for_nautilus(all_options, symbol, EXCHANGE, timestamp):
                oc_rows.append(row)
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
            continue

        if oc_rows:
            df = pd.DataFrame(oc_rows)
            out_path = os.path.join(OUTPUT_DIR, f"{symbol}_{today_str}.csv")
            if os.path.exists(out_path):
                df.to_csv(out_path, mode='a', header=False, index=False)
            else:
                df.to_csv(out_path, index=False)
            print(f"Saved {len(oc_rows)} rows to {out_path}")
        else:
            print(f"No data for {symbol}")

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"Error: {e}\n")