import os

# Read API credentials from environment variables — never hardcode keys
# in source. See README.md "Setup" for how to export these.
api = os.environ.get('BINANCE_API_KEY', '')
api_secret = os.environ.get('BINANCE_API_SECRET', '')
