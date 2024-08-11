import pandas as pd
import requests
from binance.client import Client
from config import api, api_secret


client = Client(api, api_secret, tld="com", testnet=True)

class Bot:
    def __init__(self, symbol, no_of_decimals, volume, proportion, tp, n):
        self.symbol = symbol
        self.no_of_decimals = no_of_decimals
        self.volume = volume
        self.proportion = proportion
        self.tp = tp
        self.n = n

    def get_balance(self):
        x = client.futures_account()
        df = pd.DataFrame(x['assets'])
        print(df)

    def get_current_price(self, symbol):
        response = requests.get(f'https://testnet.binancefuture.com/fapi/v1/ticker/price?symbol={symbol}')
        price = float(response.json()['price'])
        return price
