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

def fetch_options_data(instrument_key, lookback_days=14):
    """Fetch historical LTP data for an instrument from TimescaleDB."""
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        end_time = datetime.now(pytz.timezone("Asia/Kolkata"))
        start_time = end_time - timedelta(days=lookback_days)

        cursor.execute("""
            SELECT time, ltp
            FROM market_data
            WHERE instrument_key = %s AND time >= %s AND time <= %s
            ORDER BY time ASC;
        """, (instrument_key, start_time, end_time))

        data = cursor.fetchall()
        cursor.close()
        conn.close()

        if not data:
            print(f"No data found for {instrument_key}")
            return None, None

        times, prices = zip(*data)
        return np.array(prices, dtype=np.float64), times
    except Exception as e:
        print(f"Database error: {e}")
        return None, None

def compute_indicators(prices):
    """Compute RSI and MACD indicators using TA-Lib."""
    if prices is None or len(prices) < 14:
        return None, None, None

    # Compute RSI (14-period)
    rsi = talib.RSI(prices, timeperiod=14)

    # Compute MACD (12, 26, 9)
    macd, signal, _ = talib.MACD(prices, fastperiod=12, slowperiod=26, signalperiod=9)

    return rsi, macd, signal

def generate_signals(instrument_key, prices, times):
    """Generate buy/sell signals based on RSI and MACD."""
    rsi, macd, signal = compute_indicators(prices)
    if rsi is None:
        return

    signals = []
    for i in range(1, len(prices)):
        # Buy: RSI < 30 (oversold) and MACD crosses above signal
        if rsi[i] < 30 and macd[i] > signal[i] and macd[i-1] <= signal[i-1]:
            signal_msg = f"BUY {instrument_key} at {prices[i]:.2f} (RSI: {rsi[i]:.2f}, MACD: {macd[i]:.2f})"
            signals.append(signal_msg)
            logging.info(signal_msg)
        # Sell: RSI > 70 (overbought) and MACD crosses below signal
        elif rsi[i] > 70 and macd[i] < signal[i] and macd[i-1] >= signal[i-1]:
            signal_msg = f"SELL {instrument_key} at {prices[i]:.2f} (RSI: {rsi[i]:.2f}, MACD: {macd[i]:.2f})"
            signals.append(signal_msg)
            logging.info(signal_msg)

    if signals:
        print(f"Signals for {instrument_key}:")
        for signal in signals:
            print(signal)
    else:
        print(f"No signals for {instrument_key}")

def main():
    # Example: Analyze a NIFTY 50 call option
    instrument_key = "NSE_FO|NIFTY25MAY23000CE"
    prices, times = fetch_options_data(instrument_key)
    if prices is not None:
        generate_signals(instrument_key, prices, times)

if __name__ == "__main__":
    main()