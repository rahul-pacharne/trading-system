import os
from dotenv import load_dotenv
from upstox_client.api_client import ApiClient
from upstox_client.configuration import Configuration
from upstox_client.api import LoginApi, UserApi
import webbrowser
import sys

# Load environment variables
load_dotenv(override=True)

# Get credentials
api_key = os.getenv("UPSTOX_API_KEY")
api_secret = os.getenv("UPSTOX_API_SECRET")
redirect_uri = os.getenv("UPSTOX_REDIRECT_URI")
print(f"URL : {redirect_uri}")

# Validate credentials
if not all([api_key, api_secret, redirect_uri]):
    print("Error: Missing UPSTOX_API_KEY, UPSTOX_API_SECRET, or UPSTOX_REDIRECT_URI in .env")
    sys.exit(1)

# Initialize Upstox configuration
configuration = Configuration()
configuration.client_id = api_key
configuration.client_secret = api_secret
configuration.redirect_uri = redirect_uri
configuration.host = "https://api-v2.upstox.com"


# Create API client
client = ApiClient(configuration)

# Generate login URL
try:
    login_url = f"https://api-v2.upstox.com/login/authorization/dialog?client_id={api_key}&redirect_uri={redirect_uri}&response_type=code"
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
        api_version="2.0",
        code=auth_code,
        client_id=api_key,
        client_secret=api_secret,
        redirect_uri=redirect_uri,
        grant_type="authorization_code"
    )
    configuration.access_token = token_response.access_token
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

# Save access token for future use
try:
    with open("access_token.txt", "w") as f:
        f.write(token_response.access_token)
    print("Access token saved to access_token.txt")
except Exception as e:
    print(f"Error saving access token: {e}")