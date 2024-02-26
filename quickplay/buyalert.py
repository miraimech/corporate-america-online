import os
import json
import logging
import time
import pandas as pd
import pandas_ta as ta
import yfinance as yf
from multiprocessing import Pool

SCRIPT_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
BASE_PATH = os.path.join(SCRIPT_PATH, 'quickplay')
PLAY_PATH = os.path.join(BASE_PATH, 'play')
LOGS_PATH = os.path.join(BASE_PATH, 'logs')

if not os.path.exists(PLAY_PATH):
    os.makedirs(PLAY_PATH, exist_ok=True)
if not os.path.exists(LOGS_PATH):
    os.makedirs(LOGS_PATH, exist_ok=True)

logging.basicConfig(filename=os.path.join(LOGS_PATH, 'buyalert.log'), level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filemode='w')

class RemoteDataError(Exception):
    pass

def _download_nasdaq_symbols(timeout=30):
    nasdaq_url = 'ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt'
    try:
        return pd.read_csv(nasdaq_url, sep='|', skipfooter=1, engine='python')
    except Exception as e:
        raise RemoteDataError(f"Failed to download NASDAQ symbols: {e}")

_ticker_cache = None

def get_nasdaq_symbols(retry_count=3, timeout=30, pause=None):
    global _ticker_cache
    if _ticker_cache is None:
        while retry_count > 0:
            try:
                _ticker_cache = _download_nasdaq_symbols(timeout=timeout)
                break
            except RemoteDataError:
                retry_count -= 1
                if retry_count <= 0:
                    raise
                time.sleep(pause or timeout / 3)
    return _ticker_cache

def save_play_to_json(play, symbol):
    base_filename = f"{symbol}_"
    existing_files = [f for f in os.listdir(PLAY_PATH) if f.startswith(base_filename) and f.endswith('.json')]
    next_file_number = 1 if len(existing_files) == 0 else max([int(f.split('_')[-1].split('.')[0]) for f in existing_files]) + 1
    filename = os.path.join(PLAY_PATH, f"{base_filename}{next_file_number}.json")

    with open(filename, 'w') as json_file:
        json.dump(play, json_file, indent=4)

def process_ticker(symbol):
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="60d") 

        if hist.empty:
            logging.info(f"No data returned for {symbol}")
            return

        rsi = ta.rsi(hist['Close'], length=14).iloc[-1]

        average_volume = hist['Volume'].mean()
        last_day_volume = hist.iloc[-1]['Volume'] if 'Volume' in hist.columns else 0
        low_52week = hist['Low'].min()
        last_price = hist['Close'].iloc[-1]

        if average_volume > 1_000_000 and last_day_volume > 1.5 * average_volume:
            if rsi < 30 and last_price <= low_52week * 1.10:
                play_data = {
                    'ticker': symbol,
                    'price': float(last_price),
                    '52wk_low': float(low_52week),
                    'rsi': float(rsi),
                    'average_volume': int(average_volume),
                    'last_day_volume': int(last_day_volume)
                }
                save_play_to_json(play_data, symbol)
                logging.info(f"Ticker {symbol} meets criteria with RSI {rsi}, average volume {average_volume}, and last day volume {last_day_volume}")

    except Exception as e:
        logging.error(f"Error processing {symbol}: {e}")

def main():
    logging.info("Processing NASDAQ symbols for buy plays.")
    nasdaq_symbols = get_nasdaq_symbols()
    symbol_list = nasdaq_symbols['Symbol'].tolist()
    with Pool(4) as pool:
        pool.map(process_ticker, symbol_list)

if __name__ == "__main__":
    main()