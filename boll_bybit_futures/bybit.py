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
    def gen_signature(self, mod_params, timestamp):
        param_str = timestamp + self.api_key + '5000' + mod_params
        sign = hmac.new(bytes(self.secret_key, 'utf-8'), param_str.encode('utf-8'), hashlib.sha256).hexdigest()
        return sign

    def http_request(self, method, endpoint, params):
        """
        sends an http request to the trading platform server
        :param endpoint: url request address
        :param method: http request method(GET, POST)
        :param params: request parameters

        :return: :class:Response (requests.models.Response)
        """ 

        timestamp = str(int(time.time() * 1000))
        params_get_string = '&'.join([f'{k}={v}' for k, v in params.items()])
        params_post_json = json.dumps(params)

        if method == 'GET':
            sign = self.gen_signature(params_get_string, timestamp)
            self.headers['X-BAPI-SIGN'] = sign
            self.headers['X-BAPI-TIMESTAMP'] = timestamp
            response = requests.get(url=self.BASE_LINK + endpoint, params=params, headers=self.headers)
        elif method == 'POST':
            sign = self.gen_signature(params_get_string, timestamp)
            self.headers['X-BAPI-SIGN'] = sign
            self.headers['X-BAPI-TIMESTAMP'] = timestamp
            response = requests.post(url=self.BASE_LINK + endpoint, data=json.dumps(params), headers=self.headers)

        else:
            print('This method is not supported')
            return None      

        if response:
            response_json = response.json()
            response_headers = response.headers
            return response_json, response_headers
        else:
            return response.text, None

    def get_klines(self, symbol: str, interval: str, start: int = None, end: int = None,limit: int = 500, headers=False):
        endpoint = '/v5/market/kline'
        method = 'GET'
        params = {
            'category': self.category,
            'symbol': symbol,
            'interval': interval,
            'limit': limit
        }
        if start:
            params['start'] = start
        if end:
            params['end'] = end

        response_json, response_headers = self.http_request(method=method, endpoint=endpoint, params=params)
        if headers:
            return response_json, response_headers
        else:
            return response_json   
