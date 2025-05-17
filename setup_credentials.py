import os
import keyring
from dotenv import load_dotenv

# Load existing .env file
load_dotenv()

# Get credentials from .env
api_key = os.getenv("UPSTOX_API_KEY")
api_secret = os.getenv("UPSTOX_API_SECRET")
redirect_uri = os.getenv("UPSTOX_REDIRECT_URI")

# Get access token from file
try:
    with open("access_token.txt", "r") as f:
        access_token = f.read().strip()
except FileNotFoundError:
    access_token = None

# Store credentials in keyring
service_name = "prompt_trader_upstox"
if api_key:
    keyring.set_password(service_name, "api_key", api_key)
    print("Stored UPSTOX_API_KEY in keyring")
if api_secret:
    keyring.set_password(service_name, "api_secret", api_secret)
    print("Stored UPSTOX_API_SECRET in keyring")
if redirect_uri:
    keyring.set_password(service_name, "redirect_uri", redirect_uri)
    print("Stored UPSTOX_REDIRECT_URI in keyring")
if access_token:
    keyring.set_password(service_name, "access_token", access_token)
    print("Stored access_token in keyring")
else:
    print("Warning: No access_token found in access_token.txt")