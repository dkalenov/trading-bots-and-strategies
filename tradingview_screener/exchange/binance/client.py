"""
Low-level signed REST client for Binance. Deliberately not using
python-binance's wrapper - plain requests + hmac is fully auditable and
won't silently break when a third-party wrapper changes its method
signatures between versions.

This class only knows how to sign and send requests. It has no idea what
an order or a kline is - that's exchange/binance/futures.py, which
subclasses this. Mirrors the reference architecture's split between
Client (transport) and Futures(Client) (domain methods).
"""
from __future__ import annotations
import hashlib
import hmac
import time
from urllib.parse import urlencode

import requests

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from exchange.errors import BinanceAPIError

TESTNET_URL = "https://testnet.binancefuture.com"
MAINNET_URL = "https://fapi.binance.com"


class Client:
    def __init__(self, api_key: str, api_secret: str, testnet: bool = True,
                 recv_window: int = 5000, timeout: int = 10):
        self.api_key = api_key
        self.api_secret = api_secret.encode()
        self.base_url = TESTNET_URL if testnet else MAINNET_URL
        self.recv_window = recv_window
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"X-MBX-APIKEY": api_key})

    def _sign(self, params: dict) -> str:
        query = urlencode(params, doseq=True)
        return hmac.new(self.api_secret, query.encode(), hashlib.sha256).hexdigest()

    def signed_request(self, method: str, path: str, params: dict | None = None) -> dict:
        params = dict(params or {})
        params["timestamp"] = int(time.time() * 1000)
        params["recvWindow"] = self.recv_window
        params["signature"] = self._sign(params)
        resp = self.session.request(method, self.base_url + path, params=params, timeout=self.timeout)
        return self._handle(resp)

    def public_request(self, method: str, path: str, params: dict | None = None) -> dict:
        resp = self.session.request(method, self.base_url + path, params=params or {}, timeout=self.timeout)
        return self._handle(resp)

    @staticmethod
    def _handle(resp: requests.Response):
        try:
            data = resp.json()
        except ValueError:
            resp.raise_for_status()
            raise
        if resp.status_code >= 400:
            raise BinanceAPIError(resp.status_code, data)
        return data
