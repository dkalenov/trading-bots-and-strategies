from backtesting import Backtest, Strategy
from backtesting.lib import crossover
from hyperopt import fmin, tpe, hp, Trials

class SlopingStrategy(Strategy):
    window_length = 50
    sloping_atr_length = 14
    min_space = 5
    stop_loss_multiplier = 2.0
    take_profit_multiplier = 1.0

    def init(self):
        self.slope_indicator = Sloping(
            length=self.window_length,
            atr_length=self.sloping_atr_length,
            min_space=self.min_space
        )
        self._reset_trade()

    def next(self):
        self.slope_indicator.add_kline(
            self.data.index[-1].timestamp(),
            self.data.Open[-1],
            self.data.High[-1],
            self.data.Low[-1],
            self.data.Close[-1]
        )

        signal = self.slope_indicator.get_value()
        close_price = self.data.Close[-1]

        if signal and not self.trade_active:
            current_atr = signal.atr
            self.entry_price = close_price
            self.stop_loss = self.entry_price - (self.stop_loss_multiplier * current_atr) if signal.side else self.entry_price + (self.stop_loss_multiplier * current_atr)
            self.tp1 = self.entry_price + (self.take_profit_multiplier * current_atr * 0.5) if signal.side else self.entry_price - (self.take_profit_multiplier * current_atr * 0.5)
            self.tp2 = self.entry_price + (self.take_profit_multiplier * current_atr * 1.0) if signal.side else self.entry_price - (self.take_profit_multiplier * current_atr * 1.0)
            self.tp3 = self.entry_price + (self.take_profit_multiplier * current_atr * 1.5) if signal.side else self.entry_price - (self.take_profit_multiplier * current_atr * 1.5)
            
            print(f"🚀 Index: {self.data.index[-1]}, Price: {close_price}, ATR: {current_atr:.2f}, "
                  f"Signal: {'LONG' if signal.side else 'SHORT'}, SL: {self.stop_loss:.2f}, "
                  f"TP1: {self.tp1:.2f}, TP2: {self.tp2:.2f}, TP3: {self.tp3:.2f}")

            try:
                if signal.side:
                    self.buy(sl=self.stop_loss)
                else:
                    self.sell(sl=self.stop_loss)
                self.trade_active = True
            except Exception as e:
                print(f"❌ Open position error: {e}")

        if self.trade_active and self.position:
            if not self.tp1_filled and ((self.position.is_long and close_price >= self.tp1) or (self.position.is_short and close_price <= self.tp1)):
                self.position.close(portion=0.2)
                self.tp1_filled = True
                print(f"✅ TP1: {self.data.index[-1]}, Price: {close_price:.2f}")

            if self.tp1_filled and not self.tp2_filled and ((self.position.is_long and close_price >= self.tp2) or (self.position.is_short and close_price <= self.tp2)):
                self.position.close(portion=0.5)
                self.tp2_filled = True
                self.stop_loss = self.entry_price * 1.01 if self.position.is_long else self.entry_price * 0.99
                self.position.sl = self.stop_loss
                print(f"✅ TP2: {self.data.index[-1]}, Price: {close_price:.2f}, SL moved to breakeven ({self.stop_loss:.2f})")

            if self.tp2_filled and not self.tp3_filled and ((self.position.is_long and close_price >= self.tp3) or (self.position.is_short and close_price <= self.tp3)):
                self.position.close(portion=0.3)
                self.tp3_filled = True
                print(f"✅ TP3: {self.data.index[-1]}, Price: {close_price:.2f}")
                self._reset_trade()

            if self.stop_loss is not None:
                if (self.position.is_long and close_price <= self.stop_loss) or (self.position.is_short and close_price >= self.stop_loss):
                    print(f"🛑 Stop Loss Closing: {self.data.index[-1]}, Price: {close_price:.2f}")
                    self.position.close()
                    self._reset_trade()

    def _reset_trade(self):
        self.entry_price = None
        self.stop_loss = None
        self.tp1 = None
        self.tp2 = None
        self.tp3 = None
        self.tp1_filled = False
        self.tp2_filled = False
        self.tp3_filled = False
        self.trade_active = False
