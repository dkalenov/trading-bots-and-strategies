"""Strategy: ML Candle Patterns."""

import pandas as pd
import numpy as np
from patterns import detect_candlestick_patterns
from ml_models import train_model, predict


class CandlePatternsML:
    """ML strategy based on candlestick pattern detection."""

    def __init__(self):
        self.model = None
        self.feature_cols = None
        self.metrics = None

    def prepare_features(self, df):
        """Add ML features to dataframe."""
        df = detect_candlestick_patterns(df)
        df['returns'] = df['close'].pct_change()
        df['volatility'] = df['returns'].rolling(20).std()
        df['volume_ma'] = df['volume'].rolling(20).mean()
        df['volume_ratio'] = df['volume'] / df['volume_ma']
        df['target'] = (df['close'].shift(-1) > df['close']).astype(int)
        df = df.dropna()
        return df

    def train(self, train_ratio: float = 0.7, df: pd.DataFrame = None):
        """Train ML model on historical data."""
        if df is None:
            raise ValueError("No data provided")

        df = self.prepare_features(df)
        self.feature_cols = [c for c in df.columns
                            if c not in ('target', 'open', 'high', 'low', 'close', 'volume')]
        X = df[self.feature_cols]
        y = df['target']
        split_idx = int(len(df) * train_ratio)
        X_train, X_test = X[:split_idx], X[split_idx:]
        y_train, y_test = y[:split_idx], y[split_idx:]
        self.model, self.metrics = train_model(X_train, y_train, X_test, y_test)
        return self.metrics

    def predict(self, df):
        """Generate trading signals for new data."""
        if self.model is None:
            raise ValueError("Model not trained. Call train() first.")
        df = self.prepare_features(df)
        X = df[self.feature_cols]
        signals = predict(self.model, X)
        return pd.Series(signals, index=df.index, name='signal')

    def get_last_signal(self, df):
        """Get the most recent signal."""
        signals = self.predict(df)
        last_signal = signals.iloc[-1]
        return {
            'signal': int(last_signal),
            'direction': 'LONG' if last_signal == 1 else 'SHORT',
            'timestamp': signals.index[-1],
        }
