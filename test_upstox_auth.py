import keyring
import webbrowser
import sys
from upstox_client.api_client import ApiClient
from upstox_client.configuration import Configuration
from upstox_client.api import LoginApi, UserApi

# Get credentials from keyring
service_name = "prompt_trader_upstox"
api_key = keyring.get_password(service_name, "api_key")
api_secret = keyring.get_password(service_name, "api_secret")
redirect_uri = keyring.get_password(service_name, "redirect_uri")

# Validate credentials
if not all([api_key, api_secret, redirect_uri]):
    print("Error: Missing UPSTOX_API_KEY, UPSTOX_API_SECRET, or UPSTOX_REDIRECT_URI in keyring")
    sys.exit(1)

# Initialize Upstox configuration
configuration = Configuration()
configuration.client_id = api_key
configuration.client_secret = api_secret
configuration.redirect_uri = redirect_uri
configuration.host = "https://api.upstox.com/v2"

# Create API client
client = ApiClient(configuration)

# Generate login URL
try:
    login_url = f"https://api.upstox.com/v2/login/authorization?client_id={api_key}&redirect_uri={redirect_uri}&response_type=code"
    print(f"Open this URL in your browser: {login_url}")
except Exception as e:
    print(f"Error generating login URL: {e}")
    sys.exit(1)

# Open browser for authentication
webbrowser.open(login_url)

# Prompt for authorization code
auth_code = input("Enter the authorization code from the redirect URL: ").strip()
if not auth_code:
    print("Error: No authorization code provided")
    sys.exit(1)

# Exchange authorization code for access token
try:
    login_api = LoginApi(client)
    token_response = login_api.token(
        code=auth_code,
        client_id=api_key,
        client_secret=api_secret,
        redirect_uri=redirect_uri,
        grant_type="authorization_code",
        exchanges=["NSE"],
        api_version="2.0"
    )
    configuration.access_token = token_response.access_token
    # Store access token in keyring
    keyring.set_password(service_name, "access_token", token_response.access_token)
except Exception as e:
    print(f"Error obtaining access token: {e}")
    sys.exit(1)

# Test authentication by fetching profile
try:
    user_api = UserApi(client)
    profile = user_api.get_profile(api_version="2.0")
    print("Authentication successful! Profile:", profile)
except Exception as e:
    print(f"Error fetching profile: {e}")
    sys.exit(1)

print("Access token updated in keyring")