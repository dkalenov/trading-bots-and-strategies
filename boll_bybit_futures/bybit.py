import hashlib
import hmac
import json
import requests
import time

class BybitApi:
    BASE_LINK = 'https://api.bybit.com'

    def __init__(self, api_key=None, secret_key=None, futures=False):
        self.api_key = api_key
        self.secret_key = secret_key
        self.futures = futures

        if self.futures:
            self.category = 'linear'
        else:
            self.category = 'spot'

        self.headers = {
            'X-BAPI-API-KEY': self.api_key,
            'X-BAPI-RECV-WINDOW': '5000',
        }
