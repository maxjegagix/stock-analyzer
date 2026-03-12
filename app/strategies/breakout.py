def breakout(df):
    last = df.iloc[-1]
    prev_high = df['high'].iloc[-20:-1].max()
    return last['close'] > prev_high