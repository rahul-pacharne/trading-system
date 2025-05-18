import psycopg2
import numpy as np
import talib
from datetime import datetime, timedelta
import pytz
import logging

# Set up logging
logging.basicConfig(filename='/home/rahul/Documents/prompt_trader/trading_signals.log', level=logging.INFO, 
                    format='%(asctime)s - %(message)s')

# Database connection parameters
db_params = {
    "dbname": "prompt_trader",
    "user": "trader",
    "password": "secure_password",
    "host": "localhost",
    "port": "5432"
}

def fetch_options_instruments():
    """Fetch all NIFTY 50 options chain instruments from the database."""
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT instrument_key FROM market_data WHERE instrument_key LIKE 'NSE_FO%';")
        instruments = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return instruments
    except Exception as e:
        print(f"Database error fetching instruments: {e}")
        return []

def fetch_options_data(instrument_key, lookback_days=14):
    """Fetch historical LTP data for an instrument from TimescaleDB."""
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        end_time = datetime.now(pytz.timezone("Asia/Kolkata"))
        start_time = end_time - timedelta(days=lookback_days)

        cursor.execute("""
            SELECT time, ltp, high, low, open
            FROM market_data
            WHERE instrument_key = %s AND time >= %s AND time <= %s
            ORDER BY time ASC;
        """, (instrument_key, start_time, end_time))

        data = cursor.fetchall()
        cursor.close()
        conn.close()

        if not data:
            print(f"No data found for {instrument_key}")
            return None, None, None, None, None

        times, prices, highs, lows, opens = zip(*data)
        return np.array(prices, dtype=np.float64), np.array(highs, dtype=np.float64), \
               np.array(lows, dtype=np.float64), np.array(opens, dtype=np.float64), times
    except Exception as e:
        print(f"Database error: {e}")
        return None, None, None, None, None

def compute_indicators(prices, highs, lows):
    """Compute RSI, MACD, and ATR indicators using TA-Lib."""
    if prices is None or len(prices) < 14:
        return None, None, None, None

    # Compute RSI (14-period)
    rsi = talib.RSI(prices, timeperiod=14)

    # Compute MACD (12, 26, 9)
    macd, signal, _ = talib.MACD(prices, fastperiod=12, slowperiod=26, signalperiod=9)

    # Compute ATR (14-period)
    atr = talib.ATR(highs, lows, prices, timeperiod=14)

    return rsi, macd, signal, atr

def store_signal(instrument_key, signal_time, signal_type, ltp, rsi, macd, atr):
    """Store trading signal in the database."""
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO trading_signals (signal_time, instrument_key, signal_type, ltp, rsi, macd, atr)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (signal_time, instrument_key) DO NOTHING;
        """, (signal_time, instrument_key, signal_type, ltp, rsi, macd, atr))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Database error storing signal: {e}")

def generate_signals(instrument_key, prices, highs, lows, times):
    """Generate buy/sell signals based on RSI, MACD, and ATR."""
    rsi, macd, signal, atr = compute_indicators(prices, highs, lows)
    if rsi is None:
        return

    # Volatility filter: Median ATR over lookback period
    median_atr = np.median(atr[-14:]) if len(atr) >= 14 else 0

    signals = []
    for i in range(1, len(prices)):
        # Volatility check: Only generate signals if current ATR > median ATR
        if atr[i] <= median_atr:
            continue

        signal_time = times[i]
        # Buy: RSI < 30 (oversold) and MACD crosses above signal
        if rsi[i] < 30 and macd[i] > signal[i] and macd[i-1] <= signal[i-1]:
            signal_msg = f"BUY {instrument_key} at {prices[i]:.2f} (RSI: {rsi[i]:.2f}, MACD: {macd[i]:.2f}, ATR: {atr[i]:.2f})"
            signals.append(signal_msg)
            logging.info(signal_msg)
            store_signal(instrument_key, signal_time, "BUY", prices[i], rsi[i], macd[i], atr[i])
        # Sell: RSI > 70 (overbought) and MACD crosses below signal
        elif rsi[i] > 70 and macd[i] < signal[i] and macd[i-1] >= signal[i-1]:
            signal_msg = f"SELL {instrument_key} at {prices[i]:.2f} (RSI: {rsi[i]:.2f}, MACD: {macd[i]:.2f}, ATR: {atr[i]:.2f})"
            signals.append(signal_msg)
            logging.info(signal_msg)
            store_signal(instrument_key, signal_time, "SELL", prices[i], rsi[i], macd[i], atr[i])

    if signals:
        print(f"Signals for {instrument_key}:")
        for signal in signals:
            print(signal)
    else:
        print(f"No signals for {instrument_key}")

def main():
    # Fetch all NIFTY 50 options chain instruments
    instruments = fetch_options_instruments()
    if not instruments:
        print("No options instruments found in database")
        return

    for instrument_key in instruments:
        prices, highs, lows, _, times = fetch_options_data(instrument_key)
        if prices is not None:
            generate_signals(instrument_key, prices, highs, lows, times)

if __name__ == "__main__":
    main()