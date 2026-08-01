"""
Bollinger Bands Strategy — Configuration module.

Loads parameters from config.ini or environment variables.
"""

import os
import configparser


DEFAULT_CONFIG = {
    'exchange': {
        'symbol': 'BTCUSDT',
        'interval': '5m',
        'start_date': '2024-01',
        'end_date': '2025-06',
    },
    'strategy': {
        'bb_timeperiod': '20',
        'bb_nbdevup': '2.0',
        'bb_nbdevdn': '2.0',
        'bb_matype': '0',
        'rsi_period': '14',
        'squeeze_atr_period': '14',
        'squeeze_threshold': '0.5',
        'take_profit_multiplier': '3.0',
        'stop_loss_multiplier': '1.5',
        'trailing_stop_pct': '2.0',
    },
    'backtest': {
        'initial_capital': '100000',
        'commission': '0.00055',  # Bybit standard non-VIP USDT-perp taker fee
        'slippage': '0.0002',
        'leverage': '20',
        'risk_pct': '1.0',
        'funding_rate': '0.0001',
    },
    'optimization': {
        'max_evals': '50',
        'params_to_optimize': 'bb_timeperiod,bb_nbdevup',
    },
    'data': {
        'klines_dir': 'klines',
        'source': 'bybit',
    },
    'database': {
        'path': 'boll_results.db',
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
        env_key = f'BOLL_{section.upper()}_{key.upper()}'
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
        return self.getfloat('backtest', 'commission', 0.00055)

    @property
    def slippage(self):
        return self.getfloat('backtest', 'slippage', 0.0002)

    @property
    def leverage(self):
        return self.getint('backtest', 'leverage', 20)

    @property
    def risk_pct(self):
        return self.getfloat('backtest', 'risk_pct', 1.0)

    @property
    def funding_rate(self):
        return self.getfloat('backtest', 'funding_rate', 0.0001)

    @property
    def max_evals(self):
        return self.getint('optimization', 'max_evals', 50)

    @property
    def klines_dir(self):
        return self.get('data', 'klines_dir', 'klines')

    @property
    def strategy_params(self):
        return {
            'bb_timeperiod': self.getint('strategy', 'bb_timeperiod', 20),
            'bb_nbdevup': self.getfloat('strategy', 'bb_nbdevup', 2.0),
            'bb_nbdevdn': self.getfloat('strategy', 'bb_nbdevdn', 2.0),
            'bb_matype': self.getint('strategy', 'bb_matype', 0),
            'rsi_period': self.getint('strategy', 'rsi_period', 14),
            'squeeze_atr_period': self.getint('strategy', 'squeeze_atr_period', 14),
            'squeeze_threshold': self.getfloat('strategy', 'squeeze_threshold', 0.5),
            'take_profit_multiplier': self.getfloat('strategy', 'take_profit_multiplier', 3.0),
            'stop_loss_multiplier': self.getfloat('strategy', 'stop_loss_multiplier', 1.5),
            'trailing_stop_pct': self.getfloat('strategy', 'trailing_stop_pct', 2.0),
        }
