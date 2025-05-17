import os
import ssl
import asyncio
import websockets
import json
import keyring
import psycopg2
from google.protobuf.json_format import MessageToDict
import upstox_client
from upstox_client.api_client import ApiClient
from upstox_client.configuration import Configuration
from upstox_client.api import WebsocketApi
import MarketDataFeed_pb2 as pb
from datetime import datetime
import pytz

# Database connection parameters
db_params = {
    "dbname": "prompt_trader",
    "user": "trader",
    "password": "secure_password",
    "host": "localhost",
    "port": "5432"
}

# Load access token from keyring
service_name = "prompt_trader_upstox"
access_token = keyring.get_password(service_name, "access_token")
if not access_token:
    print("Error: No access_token found in keyring. Run test_upstox_auth.py first.")
    exit(1)

# Initialize configuration
configuration = Configuration()
configuration.access_token = access_token
configuration.host = "https://api.upstox.com/v2"
client = ApiClient(configuration)

# Get WebSocket authorization
def get_websocket_auth():
    try:
        api_instance = WebsocketApi(client)
        response = api_instance.get_market_data_feed_authorize(api_version="2.0")
        return response.data.authorized_redirect_uri
    except Exception as e:
        print(f"Error getting WebSocket authorization: {e}")
        exit(1)

# Decode protobuf message
def decode_protobuf(buffer):
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return MessageToDict(feed_response)

# Store market data in database
def store_market_data(data):
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        ist = pytz.timezone("Asia/Kolkata")
        current_time = datetime.now(ist)

        for instrument_key, feed in data.get("feeds", {}).items():
            ltpc = feed.get("ff", {}).get("indexFf", {}).get("ltpc", {}) or \
                   feed.get("ff", {}).get("equityFf", {}).get("ltpc", {})
            volume = feed.get("ff", {}).get("indexFf", {}).get("marketOhlc", {}).get("ohlc", [{}])[0].get("volume", 0) or \
                     feed.get("ff", {}).get("equityFf", {}).get("marketOhlc", {}).get("ohlc", [{}])[0].get("volume", 0)
            last_close = feed.get("ff", {}).get("indexFf", {}).get("lastClose", 0) or \
                         feed.get("ff", {}).get("equityFf", {}).get("lastClose", 0)

            # Extract options-specific fields
            strike_price = None
            option_type = None
            open_interest = None
            expiry_date = None
            if "NSE_FO" in instrument_key:
                parts = instrument_key.split("|")[1].split("NIFTY")
                if len(parts) > 1:
                    option_part = parts[1]
                    if "CE" in option_part or "PE" in option_part:
                        option_type = "CE" if "CE" in option_part else "PE"
                        strike_price = float(option_part[:-2])  # e.g., "23000"
                        expiry_date = "2025-05-29"  # Example; enhance with API/CSV
                    open_interest = feed.get("ff", {}).get("equityFf", {}).get("marketLevel", {}).get("bids", {}).get("bidsAsks", [{}])[0].get("quantity", 0)

            cursor.execute("""
                INSERT INTO market_data (
                    time, instrument_key, ltp, volume, last_trade_time, last_close,
                    strike_price, option_type, open_interest, expiry_date
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (time, instrument_key) DO UPDATE
                SET ltp = EXCLUDED.ltp,
                    volume = EXCLUDED.volume,
                    last_trade_time = EXCLUDED.last_trade_time,
                    last_close = EXCLUDED.last_close,
                    strike_price = EXCLUDED.strike_price,
                    option_type = EXCLUDED.option_type,
                    open_interest = EXCLUDED.open_interest,
                    expiry_date = EXCLUDED.expiry_date;
            """, (
                current_time,
                instrument_key,
                ltpc.get("ltp", 0.0),
                volume,
                ltpc.get("ltt", 0),
                last_close,
                strike_price,
                option_type,
                open_interest,
                expiry_date
            ))

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Stored data for {len(data.get('feeds', {}))} instruments")
    except Exception as e:
        print(f"Database error: {e}")

# WebSocket connection
async def fetch_market_data():
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    ws_url = get_websocket_auth()
    async with websockets.connect(ws_url, ssl=ssl_context) as websocket:
        print("WebSocket connection established")

        # Subscribe to NIFTY 50 and options chain
        data = {
            "guid": "market-data-feed",
            "method": "sub",
            "data": {
                "mode": "full",
                "instrumentKeys": [
                    "NSE_INDEX|Nifty 50",
                    "NSE_EQ|INE009A01021",  # Reliance Industries
                    "NSE_FO|NIFTY25MAY23000CE",  # Call option
                    "NSE_FO|NIFTY25MAY23000PE",  # Put option
                    "NSE_FO|NIFTY25MAY23500CE",  # Call option
                    "NSE_FO|NIFTY25MAY23500PE"   # Put option
                ]
            }
        }
        await websocket.send(json.dumps(data))
        print("Subscribed to NIFTY 50, Reliance Industries, and NIFTY options chain")

        # Receive and decode data
        try:
            while True:
                message = await websocket.recv()
                decoded_data = decode_protobuf(message)
                print("Market Data:", decoded_data)
                store_market_data(decoded_data)
        except Exception as e:
            print(f"WebSocket error: {e}")

# Run the WebSocket client
if __name__ == "__main__":
    asyncio.run(fetch_market_data())