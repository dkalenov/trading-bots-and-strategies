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

        def sell_limit(self, symbol, volume, price):
        output = client.futures_create_order(
            symbol=symbol,
            side=Client.SIDE_SELL,
            type=Client.FUTURE_ORDER_TYPE_LIMIT,
            timeInForce=Client.TIME_IN_FORCE_GTC,
            quantity=volume,
            price=price,
        )
        print(output)

    def buy_limit(self, symbol, volume, price):
        output = client.futures_create_order(
            symbol=symbol,
            side=Client.SIDE_BUY,
            type=Client.FUTURE_ORDER_TYPE_LIMIT,
            timeInForce=Client.TIME_IN_FORCE_GTC,
            quantity=volume,
            price=price,
        )
        print(output)

    def close_orders(self, symbol):
        x = client.futures_get_open_orders(symbol=symbol)
        df = pd.DataFrame(x)
        for index in df.index:
            client.futures_cancel_order(symbol=symbol, orderId=df["orderId"][index])

    def close_buy_orders(self, symbol):
        x = client.futures_get_open_orders(symbol=symbol)
        df = pd.DataFrame(x)
        df = df[df["side"] == "BUY"]
        for index in df.index:
            client.futures_cancel_order(symbol=symbol, orderId=df["orderId"][index])

    def close_sell_orders(self, symbol):
        x = client.futures_get_open_orders(symbol=symbol)
        df = pd.DataFrame(x)
        df = df[df["side"] == "SELL"]
        for index in df.index:
            client.futures_cancel_order(symbol=symbol, orderId=df["orderId"][index])

    def get_direction(self, symbol):
        x = client.futures_position_information(symbol=symbol)
        df = pd.DataFrame(x)
        if float(df["positionAmt"].sum()) > 0:
            return "LONG"
        if float(df["positionAmt"].sum()) < 0:
            return "SHORT"
        else:
            return "FLAT"

    def get_mark_price(self, symbol):
        x = client.get_symbol_ticker(symbol=symbol)
        price = float(x["price"])
        return price
