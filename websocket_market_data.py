import os
import ssl
import asyncio
import websockets
import json
import keyring
from google.protobuf.json_format import MessageToDict
import upstox_client
from upstox_client.api_client import ApiClient
from upstox_client.configuration import Configuration
from upstox_client.api import WebsocketApi
import MarketDataFeed_pb2 as pb

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

# WebSocket connection
async def fetch_market_data():
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    ws_url = get_websocket_auth()
    async with websockets.connect(ws_url, ssl=ssl_context) as websocket:
        print("WebSocket connection established")

        # Subscribe to instruments
        data = {
            "guid": "market-data-feed",
            "method": "sub",
            "data": {
                "mode": "full",
                "instrumentKeys": [
                    "NSE_INDEX|Nifty 50",
                    "NSE_EQ|INE009A01021"  # Reliance Industries
                ]
            }
        }
        await websocket.send(json.dumps(data))
        print("Subscribed to NIFTY 50 and Reliance Industries")

        # Receive and decode data
        try:
            while True:
                message = await websocket.recv()
                decoded_data = decode_protobuf(message)
                print("Market Data:", decoded_data)
        except Exception as e:
            print(f"WebSocket error: {e}")

# Run the WebSocket client
if __name__ == "__main__":
    asyncio.run(fetch_market_data())