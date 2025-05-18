import psycopg2
import keyring
import time
from datetime import datetime
import pytz
import logging
from upstox_client.api_client import ApiClient
from upstox_client.configuration import Configuration
from upstox_client.api import OrderApi

# Set up logging
logging.basicConfig(filename='/home/rahul/Documents/prompt_trader/trade_execution.log', level=logging.INFO, 
                    format='%(asctime)s - %(message)s')

# Database connection parameters
db_params = {
    "dbname": "prompt_trader",
    "user": "trader",
    "password": "secure_password",
    "host": "localhost",
    "port": "5432"
}

# Upstox API setup
service_name = "prompt_trader_upstox"
access_token = keyring.get_password(service_name, "access_token")
if not access_token:
    print("Error: No access_token found in keyring. Run test_upstox_auth.py first.")
    exit(1)

configuration = Configuration()
configuration.access_token = access_token
configuration.host = "https://api.upstox.com/v2"
client = ApiClient(configuration)
order_api = OrderApi(client)

def fetch_new_signals(last_checked):
    """Fetch new trading signals since last_checked time."""
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT signal_time, instrument_key, signal_type, ltp
            FROM trading_signals
            WHERE signal_time > %s AND instrument_key LIKE 'NSE_FO%'
            ORDER BY signal_time ASC;
        """, (last_checked,))
        signals = cursor.fetchall()
        cursor.close()
        conn.close()
        return signals
    except Exception as e:
        print(f"Database error fetching signals: {e}")
        return []

def place_order(instrument_key, signal_type, ltp):
    """Place a market order via Upstox API."""
    try:
        order_data = {
            "quantity": 25,  # NIFTY lot size
            "product": "I",  # Intraday
            "validity": "DAY",
            "price": 0,  # Market order
            "tag": "PromptTrader",
            "instrument_token": instrument_key,
            "order_type": "MARKET",
            "transaction_type": signal_type,  # BUY or SELL
            "disclosed_quantity": 0,
            "trigger_price": 0,
            "is_amo": False
        }
        response = order_api.place_order(order_data, api_version="2.0")
        order_id = response.data.order_id
        status = response.data.status
        log_msg = f"Placed {signal_type} order for {instrument_key} at {ltp:.2f}, Order ID: {order_id}, Status: {status}"
        print(log_msg)
        logging.info(log_msg)
        return order_id, status
    except Exception as e:
        log_msg = f"Error placing {signal_type} order for {instrument_key}: {e}"
        print(log_msg)
        logging.error(log_msg)
        return None, "REJECTED"

def store_order(order_time, instrument_key, order_type, quantity, price, order_id, status):
    """Store executed order in the database."""
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO executed_orders (order_time, instrument_key, order_type, quantity, price, order_id, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_time, instrument_key) DO NOTHING;
        """, (order_time, instrument_key, order_type, quantity, price, order_id, status))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Database error storing order: {e}")

def main():
    last_checked = datetime.now(pytz.timezone("Asia/Kolkata")) - timedelta(minutes=5)
    
    while True:
        # Check for new signals every 30 seconds
        signals = fetch_new_signals(last_checked)
        current_time = datetime.now(pytz.timezone("Asia/Kolkata"))
        
        for signal_time, instrument_key, signal_type, ltp in signals:
            # Place order
            order_id, status = place_order(instrument_key, signal_type, ltp)
            # Store order details
            store_order(signal_time, instrument_key, signal_type, 25, ltp, order_id, status)
        
        last_checked = current_time
        time.sleep(30)  # Wait before next check

if __name__ == "__main__":
    main()