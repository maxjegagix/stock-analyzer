import pandas as pd

def moving_average(df, period=20):
    return df['close'].rolling(period).mean()