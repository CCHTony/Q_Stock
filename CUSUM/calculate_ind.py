import pandas as pd
import numpy as np

######## calculate_indicator ############

# 特別處理: 如果分母為 0 的情況
def safe_division(numerator, denominator):
    with np.errstate(divide='ignore', invalid='ignore'):
        result = np.where(denominator == 0, 
                          np.where(numerator == 0, 1, np.where(numerator > 0, np.inf, -np.inf)), 
                          numerator / denominator)
    return result

def map_to_minus_one_to_one(x):
    """
    將輸入值 x 映射到 -1 到 1 之間。
    映射方法為 arctan(x) / (π/2)。
    """
    return np.arctan(x) / (np.pi / 2)

### sma ###
def calculate_sma(data, period):
    return data.rolling(window=period).mean()

### rsi_ema ###
def calculate_rsi(close, period):
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).ewm(span=period, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(span=period, adjust=False).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

### bollinger ###
def calculate_bollinger(close, window):
    mean = close.ewm(span=window, adjust=False).mean()
    std = close.ewm(span=window, adjust=False).std()
    upper_band = mean + (std * 2)
    lower_band = mean - (std * 2)
    partial_b = (close - lower_band) / (upper_band - lower_band)
    return upper_band, mean, lower_band, partial_b

### kdj ###
def calculate_kdj(data, n=9):
    # 計算n日內的最高價和最低價
    low_min = data['low'].rolling(window=n, min_periods=1).min()
    high_max = data['high'].rolling(window=n, min_periods=1).max()

    # 避免除以零，加入一個極小值
    epsilon = 1e-10
    rsv = ((data['close'] - low_min) / (high_max - low_min + epsilon) * 100)

    # 初始化K, D值
    K = pd.Series(0.0, index=data.index)
    D = pd.Series(0.0, index=data.index)

    # 使用RSV計算K線和D線
    for i in range(len(data)):
        if i == 0:
            K[i] = 50  # 可以設定K值的初始值為50
            D[i] = 50  # 可以設定D值的初始值為50
        else:
            K[i] = 2/3 * K[i-1] + 1/3 * rsv[i]
            D[i] = 2/3 * D[i-1] + 1/3 * K[i]

    # 計算J線
    J = 3 * K - 2 * D
    

    return K, D, J

### macd ###
def calculate_macd(data, short_window=12, long_window=26, signal_window=9):
    # 計算短期EMA
    short_ema = data['close'].ewm(span=short_window, adjust=False).mean()
    
    # 計算長期EMA
    long_ema = data['close'].ewm(span=long_window, adjust=False).mean()

    # 計算MACD線
    macd = short_ema - long_ema
    
    # 計算信號線
    signal = macd.ewm(span=signal_window, adjust=False).mean()
    
    # 計算柱狀圖
    histogram = macd - signal
    
    return macd, signal, histogram

################ DMI ###############
def calculate_dmi(data, window=14):
    df = data.copy()
    previousHigh = df['high'].shift(1)
    previousLow = df['low'].shift(1)
    previousClose = df['close'].shift(1)
    
        # 計算TR（True Range）
    def calculate_tr(row):
        try:
            return max(row['high'] - row['low'], 
                       abs(row['high'] - previousClose.loc[row.name]), 
                       abs(row['low'] - previousClose.loc[row.name]))
        except Exception as e:
            print(f"Error in row {row.name}: {row}")
            print(f"Previous Close: {previousClose.loc[row.name]}")
            print(f"Exception: {e}")
            raise
    
    df['TR'] = df.apply(calculate_tr, axis=1)
    
    # 計算+DM和-DM
    df['+DM'] = np.where((df['high'] - previousHigh) > (previousLow - df['low']), 
                         np.maximum(df['high'] - previousHigh, 0), 0)
    df['-DM'] = np.where((previousLow - df['low']) > (df['high'] - previousHigh), 
                         np.maximum(previousLow - df['low'], 0), 0)
    
    # 計算EMA
    df['TR_EMA'] = df['TR'].ewm(span=window, adjust=False).mean()
    df['+DM_EMA'] = df['+DM'].ewm(span=window, adjust=False).mean()
    df['-DM_EMA'] = df['-DM'].ewm(span=window, adjust=False).mean()
    
    # 計算+DI和-DI
    df['+DI'] = 100 * (df['+DM_EMA'] / df['TR_EMA'])
    df['-DI'] = 100 * (df['-DM_EMA'] / df['TR_EMA'])
    
    # 計算DX
    df['DX'] = 100 * abs(df['+DI'] - df['-DI']) / (df['+DI'] + df['-DI'])
    
    # 計算ADX
    df['ADX'] = df['DX'].ewm(span=window, adjust=False).mean()
    
    return df['+DI'], df['-DI'], df['ADX']

def calculate_obv(close, vol):
    obv_values = np.where(close > close.shift(1), vol / 1000,
                          np.where(close < close.shift(1), -vol / 1000, 0))
    obv = pd.Series(obv_values).cumsum()
    return obv

def calculate_modified_obv(close, high, low, volume):
    
    # 修改的 OBV 計算
    condition = (high == low)
    modified_OBV_value = np.where(
        condition & (close >= close.shift(1)),
        volume,
        np.where(
            condition & (close < close.shift(1)),
            -volume,
            volume * ((close - low) / (high - low))
        )
    )
    
    modified_OBV = pd.Series(modified_OBV_value).cumsum()
    return modified_OBV

def calculate_ad(close, high, low, volume):
    mfm = np.where(high == low, 
                   np.where(close > close.shift(1), 0.5, 
                            np.where(close < close.shift(1), -0.5, 0)),
                   ((close - low) - (high - close)) / (high - low))

    mfv = mfm * volume
    ad = pd.Series(mfv).cumsum()

    return ad

def calculate_pvt(close, vol):
    pvt = np.zeros(len(close))
    
    # 计算 PVT
    price_change_ratio = (close - close.shift(1)) / close.shift(1)
    pvt[1:] = vol[1:] * price_change_ratio[1:]
    pvt = pd.Series(pvt).cumsum()
    
    return pvt

def calculate_momentum(data, window):
    mom = data['close'] - data['close'].shift(window)
    return mom

def calculate_wr(close, high, low, window):
    highest_high = high.rolling(window=window).max()
    lowest_low = low.rolling(window=window).min()
    wr = (close - highest_high) / (highest_high - lowest_low) 
    
    return wr

def calculate_intraday_momentum_index(data, window):
    red_k = np.where(data['close'] > data['open'], data['close'] - data['open'], 0)
    green_k = np.where(data['close'] < data['open'], data['open'] - data['close'], 0)
    red_k_sum = pd.Series(red_k).rolling(window=window).sum()
    green_k_sum = pd.Series(green_k).rolling(window=window).sum()
    imi = (red_k_sum / (red_k_sum + green_k_sum)) * 100
    
    return imi

def calculate_ar(data, window):
    ar = np.where(data['open'] != data['low'], (data['high'] - data['open']) / np.abs(data['open'] - data['low']), 0)
    ar_sum = pd.Series(ar).rolling(window=window).sum()
    
    return ar_sum

def calculate_br(data, window):
    br = (data['high'] - data['close'].shift(1)) / np.abs(data['close'].shift(1) - data['low'])
    br_sum = pd.Series(br).rolling(window=window).sum()
    
    return br_sum

def calculate_atr_sma(data, window=14):
    high_low = data['high'] - data['low']
    high_close_prev = np.abs(data['high'] - data['close'].shift(1))
    low_close_prev = np.abs(data['low'] - data['close'].shift(1))
    
    tr = np.maximum.reduce([high_low, high_close_prev, low_close_prev])
    atr = pd.Series(tr).rolling(window=window).mean()
    
    return atr

############ realtime ########################

def calculate_rt_sma(data, period):
    sma_values = pd.Series([None] * len(data))  # 初始化為 None 的 Series
    for i in range(len(data)):
        if i < period:
            sma_values[i] = None
        else:
            if i + 1 < len(data):  # 確保隔天的 open 價格存在
                sma_window = data['close'][i-period+2:i+1].tolist() + [data['open'][i+1]]
                sma_values[i] = sum(sma_window) / len(sma_window)
            else:
                sma_values[i] = None  # 如果隔天的 open 價格不存在，無法計算即時 SMA
    return sma_values

def calculate_rt_ema(data, period):
    ema_values = pd.Series([None] * len(data))  # 初始化為 None 的 Series
    multiplier = 2 / (period + 1)
    
    for i in range(len(data)):
        if i < period - 1:
            ema_values[i] = None  # 週期不足時無法計算 EMA
        elif i == period - 1:
            # 第一個 EMA 是 SMA
            sma = sum(data['close'][0:period]) / period
            ema_values[i] = sma
        else:
            # 即時 EMA 計算
            if i + 1 < len(data):  # 確保隔天的 open 價格存在
                rt_price = data['open'][i+1]  # 即時價格為當前交易日的開盤價
                ema_values[i] = (rt_price - ema_values[i - 1]) * multiplier + ema_values[i - 1]
            else:
                ema_values[i] = None  # 無法取得即時價格時，不計算EMA
    
    return ema_values


