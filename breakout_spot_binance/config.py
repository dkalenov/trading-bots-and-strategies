"""
Breakout Spot Strategy — Configuration module.

Loads parameters from config.ini or environment variables.
"""

import os
import configparser


DEFAULT_CONFIG = {
    'exchange': {
        'symbol': 'BTCUSDT',
        'interval': '4h',
        'start_date': '2023-01',
        'end_date': '2025-06',
    },
    'strategy': {
        'lookback': '20',
        'volume_multiplier': '2.0',
        'atr_period': '14',
        'stop_loss_multiplier': '1.5',
        'trailing_stop_pct': '3.0',
        'min_volume_usdt': '1000000',
    },
    'backtest': {
        'initial_capital': '10000',
        'commission': '0.001',
        'slippage': '0.0005',
    },
    'optimization': {
        'max_evals': '50',
    },
    'data': {
        'klines_dir': 'klines',
    },
    'database': {
        'path': 'breakout_results.db',
    },
}


class Config:
    def __init__(self, config_path='config.ini'):
        self.parser = configparser.ConfigParser()
        if os.path.exists(config_path):
            self.parser.read(config_path)
        else:
            self.parser.read_dict(DEFAULT_CONFIG)

    def get(self, section, key, fallback=None):
        env_key = f'BREAKOUT_{section.upper()}_{key.upper()}'
        env_val = os.environ.get(env_key)
        if env_val is not None:
            return env_val
        return self.parser.get(section, key, fallback=fallback or '')

    def getint(self, section, key, fallback=0):
        return int(self.get(section, key, fallback=str(fallback)))

    def getfloat(self, section, key, fallback=0.0):
        return float(self.get(section, key, fallback=str(fallback)))

    @property
    def symbol(self):
        return self.get('exchange', 'symbol')

    @property
    def interval(self):
        return self.get('exchange', 'interval')

    @property
    def start_date(self):
        return self.get('exchange', 'start_date')

    @property
    def end_date(self):
        return self.get('exchange', 'end_date')

    @property
    def initial_capital(self):
        return self.getfloat('backtest', 'initial_capital', 10000)

    @property
    def commission(self):
        return self.getfloat('backtest', 'commission', 0.001)

    @property
    def slippage(self):
        return self.getfloat('backtest', 'slippage', 0.0005)

    @property
    def max_evals(self):
        return self.getint('optimization', 'max_evals', 50)
