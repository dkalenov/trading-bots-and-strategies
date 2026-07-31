"""
UTBot Strategy — Configuration module.

Loads parameters from config.ini or environment variables.
"""

import os
import configparser


DEFAULT_CONFIG = {
    'exchange': {
        'symbol': 'BTCUSDT',
        'interval': '1h',
        'start_date': '2024-01',
        'end_date': '2024-12',
    },
    'strategy': {
        'key_value': '8',
        'atr_period': '10',
        'take_profit_multiplier': '3.0',
        'stop_loss_multiplier': '1.5',
    },
    'backtest': {
        'initial_capital': '100000',
        'commission': '0.0005',
        'slippage': '0.0001',
        'leverage': '1',
        'risk_pct': '0.01',
        'funding_rate': '0.0001',
        'funding_interval_bars': '8',
    },
    'optimization': {
        'max_evals': '50',
        'params_to_optimize': 'key_value,atr_period',
    },
    'data': {
        'klines_dir': 'klines',
    },
    'database': {
        'path': 'utbot_results.db',
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
        env_key = f'UTBOT_{section.upper()}_{key.upper()}'
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
        return self.getfloat('backtest', 'initial_capital', 100000)

    @property
    def commission(self):
        return self.getfloat('backtest', 'commission', 0.0005)

    @property
    def slippage(self):
        return self.getfloat('backtest', 'slippage', 0.0001)

    @property
    def leverage(self):
        return self.getint('backtest', 'leverage', 1)

    @property
    def risk_pct(self):
        return self.getfloat('backtest', 'risk_pct', 0.01)

    @property
    def funding_rate(self):
        return self.getfloat('backtest', 'funding_rate', 0.0001)

    @property
    def funding_interval_bars(self):
        return self.getint('backtest', 'funding_interval_bars', 8)

    @property
    def max_evals(self):
        return self.getint('optimization', 'max_evals', 50)

    @property
    def strategy_params(self):
        return {
            'key_value': self.getint('strategy', 'key_value', 8),
            'atr_period': self.getint('strategy', 'atr_period', 10),
            'take_profit_multiplier': self.getfloat('strategy', 'take_profit_multiplier', 3.0),
            'stop_loss_multiplier': self.getfloat('strategy', 'stop_loss_multiplier', 1.5),
            'trailing_stop_pct': self.getfloat('strategy', 'trailing_stop_pct', 2.0),
        }
