import requests
import time
import json
from requests.exceptions import RequestException

NSE_URL = 'https://www.nseindia.com/api/option-chain-indices?symbol={symbol}'
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/option-chain",
    "X-Requested-With": "XMLHttpRequest",
    "Connection": "keep-alive"
}

REQUIRED_FIELDS = [
    'strikePrice', 'expiryDate', 'bidprice', 'askPrice', 'lastPrice', 'totalTradedVolume', 'openInterest'
]


def fetch_all_option_chain(symbol, retries=3, backoff=2):
    HOMEPAGE_URL = "https://www.nseindia.com/option-chain"
    API_URL = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"
    for attempt in range(retries):
        try:
            session = requests.Session()
            print("Visiting homepage to set cookies...")
            res = session.get(HOMEPAGE_URL, headers=HEADERS, timeout=10)
            print(f"Homepage status: {res.status_code}")
            time.sleep(2)
            print("Hitting the API...")
            response = session.get(API_URL, headers=HEADERS, timeout=10)
            print(f"API Status: {response.status_code}")
            content_type = response.headers.get("Content-Type", "")
            print("Content-Type:", content_type)
            if response.status_code == 200:
                if "application/json" in content_type:
                    try:
                        json_data = response.json()
                        data = json_data['records']['data']
                        expiry_dates = json_data['records']['expiryDates']
                        underlying_value = json_data['records'].get('underlyingValue')
                        print("JSON fetched successfully.")
                        return {
                            'data': data, 
                            'expiry_dates': expiry_dates,
                            'underlyingValue': underlying_value
                        }
                    except json.JSONDecodeError:
                        print("JSONDecodeError - Invalid JSON format.")
                        print("Raw Response:")
                        print(response.text[:1000])
                        raise
                else:
                    print("Expected JSON but got:", content_type)
                    print("Raw HTML/Other:")
                    print(response.text[:1000])
                    raise Exception("NSE returned unexpected content format.")
            else:
                raise Exception(f"Failed to fetch data, status code: {response.status_code}")
        except (RequestException, ValueError, Exception) as e:
            if attempt < retries - 1:
                time.sleep(backoff ** attempt)
            else:
                raise RuntimeError(f"Failed to fetch data from NSE after {retries} attempts: {e}")

def format_for_nautilus(option_chain, underlying, exchange, timestamp):
    data = option_chain['data']
    expiry_dates = option_chain['expiry_dates']
    rows = []
    
    # Get the overall underlying value from records level
    records_underlying_value = option_chain.get('underlyingValue')
    
    for entry in data:
        strike = entry.get('strikePrice')
        expiry = entry.get('expiryDate')
        for opt_type, nse_type, label in [('CE', 'CALL', 'CALL'), ('PE', 'PUT', 'PUT')]:
            opt = entry.get(opt_type)
            if not opt or opt.get('expiryDate') != expiry:
                continue
            # Compose symbol: <UNDERLYING>.<EXCHANGE>.<TYPE>.<EXPIRY>.<STRIKE>.<PUT/CALL>
            expiry_fmt = expiry.replace('-', '')
            symbol_str = f"{underlying}.{exchange}.OPT.{expiry_fmt}.{int(strike)}.{label}"
            
            # Get underlying value from the option object first, fallback to records level
            option_underlying_value = opt.get('underlyingValue')
            final_underlying_value = option_underlying_value if option_underlying_value is not None else records_underlying_value
            
            row = {
                'timestamp': timestamp,
                'symbol': symbol_str,
                'option_type': label,
                'strike': int(strike),
                'expiry': expiry,
                'bid': float(opt.get('bidprice', 0)),
                'ask': float(opt.get('askPrice', 0)),
                'last': float(opt.get('lastPrice', 0)),
                'volume': int(opt.get('totalTradedVolume', 0)),
                'open_interest': int(opt.get('openInterest', 0)),
                'impliedVolatility': float(opt.get('impliedVolatility', 0)) if opt.get('impliedVolatility') is not None else None,
                'pchangeinOpenInterest': float(opt.get('pchangeinOpenInterest', 0)) if opt.get('pchangeinOpenInterest') is not None else None,
                'totalBuyQuantity': int(opt.get('totalBuyQuantity', 0)) if opt.get('totalBuyQuantity') is not None else None,
                'totalSellQuantity': int(opt.get('totalSellQuantity', 0)) if opt.get('totalSellQuantity') is not None else None,
                'underlyingValue': float(final_underlying_value) if final_underlying_value is not None else None,
                'underlying': opt.get('underlying') if opt.get('underlying') is not None else underlying,
                'identifier': opt.get('identifier'),
                'pChange': float(opt.get('pChange', 0)) if opt.get('pChange') is not None else None,
            }
            rows.append(row)
    return rows 