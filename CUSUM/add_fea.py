import calculate_ind as ind
import numpy as np
import pandas as pd
import os
from joblib import Parallel, delayed

def analyze_stock_data(df, shift_days=1):
        
    ############### Indicator ######################
    close = df['close']
    open = df['open']
    high = df['high']
    low = df['low']
    
    ################ sma ###############
    rsi5 = ind.calculate_rsi(close, 5)
    rsi14 = ind.calculate_rsi(close, 14)
    rsi28 = ind.calculate_rsi(close, 28)
    
    bollinger_5_upper, ema5, bollinger_5_lower, b5 = ind.calculate_bollinger(close, 5)
    bollinger_10_upper, ema10, bollinger_10_lower, b10 = ind.calculate_bollinger(close, 10)
    bollinger_22_upper, ema22, bollinger_22_lower, b22 = ind.calculate_bollinger(close, 22)
    bollinger_66_upper, ema66, bollinger_66_lower, b66 = ind.calculate_bollinger(close, 66)
    bollinger_132_upper, ema132, bollinger_132_lower, b132 = ind.calculate_bollinger(close, 132)
    bollinger_264_upper, ema264, bollinger_264_lower, b264 = ind.calculate_bollinger(close, 264)
    
    # KDJ
    K, D, J = ind.calculate_kdj(df, 14)
    
    # MACD
    macd, signal, histogram = ind.calculate_macd(df)
    
    
    ################ DMI ###############
    di_plus, di_minus, adx = ind.calculate_dmi(df)
    
    ################ OBV ###############
    obv = ind.calculate_obv(close, df['capacity']/1000)
    
    modified_obv = ind.calculate_modified_obv(close, high, low, df['capacity']/1000)
    
    ad = ind.calculate_ad(close, high, low, df['capacity']/1000)
    
    pvt = ind.calculate_pvt(close, df['capacity']/1000)
    
    mom5 = ind.calculate_momentum(df, 5)
    mom10 = ind.calculate_momentum(df, 10)
    mom20 = ind.calculate_momentum(df, 20)
    
    wr = ind.calculate_wr(close, high, low, window=5)
    
    imi = ind.calculate_intraday_momentum_index(df, window=14)
    
    # ATR
    atr = ind.calculate_atr_sma(df)
    
    ######## volome ##########
    capacity = df['capacity']
    vbollinger_5_upper, vema5, vbollinger_5_lower, vb5 = ind.calculate_bollinger(capacity, 5)
    vbollinger_10_upper, vema10, vbollinger_10_lower, vb10 = ind.calculate_bollinger(capacity, 10)
    vbollinger_22_upper, vema22, vbollinger_22_lower, vb22 = ind.calculate_bollinger(capacity, 22)
    vbollinger_66_upper, vema66, vbollinger_66_lower, vb66 = ind.calculate_bollinger(capacity, 66)
    vbollinger_132_upper, vema132, vbollinger_132_lower, vb132 = ind.calculate_bollinger(capacity, 132)
    vbollinger_264_upper, vema264, vbollinger_264_lower, vb264 = ind.calculate_bollinger(capacity, 264)
    
    # # ###### real time #########
    
    ####### K bar ##############

    amplitude = close.shift(1) * 0.2
    real_ratio = (close - open) / amplitude

    # Feature: 實體比例與昨一日比
    
    real_ratio_shift_1 = real_ratio - real_ratio.shift(1)
    real_ratio_shift_2 = real_ratio - real_ratio.shift(2)
    real_ratio_shift_3 = real_ratio - real_ratio.shift(3)


    # Feature: sma5, sma10, sma20 實體比例
    sma5_real_ratio = real_ratio.rolling(window=5).mean()
    sma10_real_ratio = real_ratio.rolling(window=10).mean()
    sma20_real_ratio = real_ratio.rolling(window=20).mean()

    # Feature: 實體比例與均線比

    real_ratio_div_sma5 = real_ratio - sma5_real_ratio
    real_ratio_div_sma10 = real_ratio - sma10_real_ratio
    real_ratio_div_sma20 = real_ratio - sma20_real_ratio


    # Feature: 均線之間的比率

    sma5_div_sma10 = sma5_real_ratio - sma10_real_ratio
    sma5_div_sma20 = sma5_real_ratio - sma20_real_ratio
    sma10_div_sma20 = sma10_real_ratio - sma20_real_ratio

    # Feature: 昨收分離率乘以實體比例
    yesterday_close_ratio_multiply_real_ratio = (close / close.shift(1)) * real_ratio

    # Feature: 五日均線分離率乘以實體比例
    sma5_ratio_multiply_real_ratio = (close / ema5) * real_ratio
 
    sma5_ratio_multiply_real_ratio_shift_1 = sma5_ratio_multiply_real_ratio - sma5_ratio_multiply_real_ratio.shift(1)
    sma5_ratio_multiply_real_ratio_shift_2 = sma5_ratio_multiply_real_ratio - sma5_ratio_multiply_real_ratio.shift(2)
    sma5_ratio_multiply_real_ratio_shift_3 = sma5_ratio_multiply_real_ratio - sma5_ratio_multiply_real_ratio.shift(3)


    # Feature: 十日均線分離率乘以實體比例
    sma10_ratio_multiply_real_ratio = (close / ema10) * real_ratio

    sma10_ratio_multiply_real_ratio_shift_1 = sma10_ratio_multiply_real_ratio - sma10_ratio_multiply_real_ratio.shift(1)
    sma10_ratio_multiply_real_ratio_shift_2 = sma10_ratio_multiply_real_ratio - sma10_ratio_multiply_real_ratio.shift(2)
    sma10_ratio_multiply_real_ratio_shift_3 = sma10_ratio_multiply_real_ratio - sma10_ratio_multiply_real_ratio.shift(3)

    # Feature: 二十日均線分離率乘以實體比例
    sma22_ratio_multiply_real_ratio = (close / ema22) * real_ratio
 
    sma22_ratio_multiply_real_ratio_shift_1 = sma22_ratio_multiply_real_ratio - sma22_ratio_multiply_real_ratio.shift(1)
    sma22_ratio_multiply_real_ratio_shift_2 = sma22_ratio_multiply_real_ratio - sma22_ratio_multiply_real_ratio.shift(2)
    sma22_ratio_multiply_real_ratio_shift_3 = sma22_ratio_multiply_real_ratio - sma22_ratio_multiply_real_ratio.shift(3)

    # Feature: %b5 表現價位於布林軌道中的位置
    bollinger_b5_multiply_real_ratio = b5 * real_ratio

    bollinger_b5_multiply_real_ratio_shift_1 = bollinger_b5_multiply_real_ratio - bollinger_b5_multiply_real_ratio.shift(1)
    bollinger_b5_multiply_real_ratio_shift_2 = bollinger_b5_multiply_real_ratio - bollinger_b5_multiply_real_ratio.shift(2)
    bollinger_b5_multiply_real_ratio_shift_3 = bollinger_b5_multiply_real_ratio - bollinger_b5_multiply_real_ratio.shift(3)

    # Feature: %b10 表現價位於布林軌道中的位置
    bollinger_b10_multiply_real_ratio = b10 * real_ratio
    
    bollinger_b10_multiply_real_ratio_shift_1 = bollinger_b10_multiply_real_ratio - bollinger_b10_multiply_real_ratio.shift(1)
    bollinger_b10_multiply_real_ratio_shift_2 = bollinger_b10_multiply_real_ratio - bollinger_b10_multiply_real_ratio.shift(2)
    bollinger_b10_multiply_real_ratio_shift_3 = bollinger_b10_multiply_real_ratio - bollinger_b10_multiply_real_ratio.shift(3)

    # Feature: %b22 表現價位於布林軌道中的位置
    bollinger_b22_multiply_real_ratio = b22 * real_ratio
  
    bollinger_b22_multiply_real_ratio_shift_1 = bollinger_b22_multiply_real_ratio - bollinger_b22_multiply_real_ratio.shift(1)
    bollinger_b22_multiply_real_ratio_shift_2 = bollinger_b22_multiply_real_ratio - bollinger_b22_multiply_real_ratio.shift(2)
    bollinger_b22_multiply_real_ratio_shift_3 = bollinger_b22_multiply_real_ratio - bollinger_b22_multiply_real_ratio.shift(3)

    # 假設 capacity 是當前成交量，vema20 是成交量的20日均線
    lot_div_vma22 = capacity / vema22

    # 計算實體比例成交量加權
    real_ratio_volume_weighted = lot_div_vma22 * real_ratio

    # Feature: 實體比例成交量加權與昨幾日的比值

    real_ratio_volume_weighted_shift_1 = real_ratio_volume_weighted - real_ratio_volume_weighted.shift(1)
    real_ratio_volume_weighted_shift_2 = real_ratio_volume_weighted - real_ratio_volume_weighted.shift(2)
    real_ratio_volume_weighted_shift_3 = real_ratio_volume_weighted - real_ratio_volume_weighted.shift(3)

    # Feature: sma5, sma10, sma20 實體比例成交量加權
    sma5_real_ratio_volume_weighted = real_ratio_volume_weighted.rolling(window=5).mean()
    sma10_real_ratio_volume_weighted = real_ratio_volume_weighted.rolling(window=10).mean()
    sma20_real_ratio_volume_weighted = real_ratio_volume_weighted.rolling(window=20).mean()

    # Feature: 實體比例成交量加權與均線比
    
    real_ratio_volume_weighted_div_sma5 = real_ratio_volume_weighted - sma5_real_ratio_volume_weighted
    real_ratio_volume_weighted_div_sma10 = real_ratio_volume_weighted - sma10_real_ratio_volume_weighted
    real_ratio_volume_weighted_div_sma20 = real_ratio_volume_weighted - sma20_real_ratio_volume_weighted

    # Feature: 均線之間的比率

    sma5_div_sma10_real_ratio_volume_weighted = sma5_real_ratio_volume_weighted - sma10_real_ratio_volume_weighted
    sma5_div_sma20_real_ratio_volume_weighted = sma5_real_ratio_volume_weighted - sma20_real_ratio_volume_weighted
    sma10_div_sma20_real_ratio_volume_weighted = sma10_real_ratio_volume_weighted - sma20_real_ratio_volume_weighted

    # Feature: 昨收分離率乘以實體比例成交量加權
    yesterday_close_ratio_multiply_real_ratio_volume_weighted = (close / close.shift(1)) * real_ratio_volume_weighted

    # Feature: 五日均線分離率乘以實體比例成交量加權
    sma5_ratio_multiply_real_ratio_volume_weighted = (close / ema5) * real_ratio_volume_weighted

    sma5_ratio_multiply_real_ratio_volume_weighted_shift_1 = sma5_ratio_multiply_real_ratio_volume_weighted - sma5_ratio_multiply_real_ratio_volume_weighted.shift(1)
    sma5_ratio_multiply_real_ratio_volume_weighted_shift_2 = sma5_ratio_multiply_real_ratio_volume_weighted - sma5_ratio_multiply_real_ratio_volume_weighted.shift(2)
    sma5_ratio_multiply_real_ratio_volume_weighted_shift_3 = sma5_ratio_multiply_real_ratio_volume_weighted - sma5_ratio_multiply_real_ratio_volume_weighted.shift(3)

    # Feature: 十日均線分離率乘以實體比例成交量加權
    sma10_ratio_multiply_real_ratio_volume_weighted = (close / ema10) * real_ratio_volume_weighted

    sma10_ratio_multiply_real_ratio_volume_weighted_shift_1 = sma10_ratio_multiply_real_ratio_volume_weighted - sma10_ratio_multiply_real_ratio_volume_weighted.shift(1)
    sma10_ratio_multiply_real_ratio_volume_weighted_shift_2 = sma10_ratio_multiply_real_ratio_volume_weighted - sma10_ratio_multiply_real_ratio_volume_weighted.shift(2)
    sma10_ratio_multiply_real_ratio_volume_weighted_shift_3 = sma10_ratio_multiply_real_ratio_volume_weighted - sma10_ratio_multiply_real_ratio_volume_weighted.shift(3)

    # Feature: 二十日均線分離率乘以實體比例成交量加權
    sma22_ratio_multiply_real_ratio_volume_weighted = (close / ema22) * real_ratio_volume_weighted

    sma22_ratio_multiply_real_ratio_volume_weighted_shift_1 = sma22_ratio_multiply_real_ratio_volume_weighted - sma22_ratio_multiply_real_ratio_volume_weighted.shift(1)
    sma22_ratio_multiply_real_ratio_volume_weighted_shift_2 = sma22_ratio_multiply_real_ratio_volume_weighted - sma22_ratio_multiply_real_ratio_volume_weighted.shift(2)
    sma22_ratio_multiply_real_ratio_volume_weighted_shift_3 = sma22_ratio_multiply_real_ratio_volume_weighted - sma22_ratio_multiply_real_ratio_volume_weighted.shift(3)

    # Feature: %b5 表現價位於布林軌道中的位置加權
    bollinger_b5_multiply_real_ratio_volume_weighted = b5 * real_ratio_volume_weighted

    bollinger_b5_multiply_real_ratio_volume_weighted_shift_1 = bollinger_b5_multiply_real_ratio_volume_weighted - bollinger_b5_multiply_real_ratio_volume_weighted.shift(1)
    bollinger_b5_multiply_real_ratio_volume_weighted_shift_2 = bollinger_b5_multiply_real_ratio_volume_weighted - bollinger_b5_multiply_real_ratio_volume_weighted.shift(2)
    bollinger_b5_multiply_real_ratio_volume_weighted_shift_3 = bollinger_b5_multiply_real_ratio_volume_weighted - bollinger_b5_multiply_real_ratio_volume_weighted.shift(3)

    # Feature: %b10 表現價位於布林軌道中的位置加權
    bollinger_b10_multiply_real_ratio_volume_weighted = b10 * real_ratio_volume_weighted

    bollinger_b10_multiply_real_ratio_volume_weighted_shift_1 = bollinger_b10_multiply_real_ratio_volume_weighted - bollinger_b10_multiply_real_ratio_volume_weighted.shift(1)
    bollinger_b10_multiply_real_ratio_volume_weighted_shift_2 = bollinger_b10_multiply_real_ratio_volume_weighted - bollinger_b10_multiply_real_ratio_volume_weighted.shift(2)
    bollinger_b10_multiply_real_ratio_volume_weighted_shift_3 = bollinger_b10_multiply_real_ratio_volume_weighted - bollinger_b10_multiply_real_ratio_volume_weighted.shift(3)

    # Feature: %b22 表現價位於布林軌道中的位置加權
    bollinger_b22_multiply_real_ratio_volume_weighted = b22 * real_ratio_volume_weighted
 
    bollinger_b22_multiply_real_ratio_volume_weighted_shift_1 = bollinger_b22_multiply_real_ratio_volume_weighted - bollinger_b22_multiply_real_ratio_volume_weighted.shift(1)
    bollinger_b22_multiply_real_ratio_volume_weighted_shift_2 = bollinger_b22_multiply_real_ratio_volume_weighted - bollinger_b22_multiply_real_ratio_volume_weighted.shift(2)
    bollinger_b22_multiply_real_ratio_volume_weighted_shift_3 = bollinger_b22_multiply_real_ratio_volume_weighted - bollinger_b22_multiply_real_ratio_volume_weighted.shift(3)

    # 計算上影線比例
    # 如果收盤價大於等於開盤價（收紅或收平），則上影線比例為 (收盤 – 最高) / 振幅
    # 如果開盤價大於收盤價（收黑），則上影線比例為 (開盤 – 最高) / 振幅
    upper_shadow_ratio = pd.Series(index=close.index)

    # 條件判斷
    red_or_flat = close >= open
    black = open > close

    # 計算上影線比例
    upper_shadow_ratio[red_or_flat] = (close - high)[red_or_flat] / amplitude[red_or_flat]
    upper_shadow_ratio[black] = (open - high)[black] / amplitude[black]

    # Feature: 上影線比例與昨日、前二日、前三日的比值

    upper_shadow_ratio_shift_1 = upper_shadow_ratio - upper_shadow_ratio.shift(1)
    upper_shadow_ratio_shift_2 = upper_shadow_ratio - upper_shadow_ratio.shift(2)
    upper_shadow_ratio_shift_3 = upper_shadow_ratio - upper_shadow_ratio.shift(3)

    
    # Feature: 昨收分離率乘以上影線比例
    yesterday_close_ratio_multiply_upper_shadow_ratio = (close / close.shift(1)) * upper_shadow_ratio

    # Feature: 五日均線分離率乘以上影線比例
    sma5_ratio_multiply_upper_shadow_ratio = (close / ema5) * upper_shadow_ratio

    sma5_ratio_multiply_upper_shadow_ratio_shift_1 = sma5_ratio_multiply_upper_shadow_ratio - sma5_ratio_multiply_upper_shadow_ratio.shift(1)
    sma5_ratio_multiply_upper_shadow_ratio_shift_2 = sma5_ratio_multiply_upper_shadow_ratio - sma5_ratio_multiply_upper_shadow_ratio.shift(2)
    sma5_ratio_multiply_upper_shadow_ratio_shift_3 = sma5_ratio_multiply_upper_shadow_ratio - sma5_ratio_multiply_upper_shadow_ratio.shift(3)

    # Feature: 十日均線分離率乘以上影線比例
    sma10_ratio_multiply_upper_shadow_ratio = (close / ema10) * upper_shadow_ratio

    sma10_ratio_multiply_upper_shadow_ratio_shift_1 = sma10_ratio_multiply_upper_shadow_ratio - sma10_ratio_multiply_upper_shadow_ratio.shift(1)
    sma10_ratio_multiply_upper_shadow_ratio_shift_2 = sma10_ratio_multiply_upper_shadow_ratio - sma10_ratio_multiply_upper_shadow_ratio.shift(2)
    sma10_ratio_multiply_upper_shadow_ratio_shift_3 = sma10_ratio_multiply_upper_shadow_ratio - sma10_ratio_multiply_upper_shadow_ratio.shift(3)

    # Feature: 二十日均線分離率乘以上影線比例
    sma20_ratio_multiply_upper_shadow_ratio = (close / ema22) * upper_shadow_ratio

    sma20_ratio_multiply_upper_shadow_ratio_shift_1 = sma20_ratio_multiply_upper_shadow_ratio - sma20_ratio_multiply_upper_shadow_ratio.shift(1)
    sma20_ratio_multiply_upper_shadow_ratio_shift_2 = sma20_ratio_multiply_upper_shadow_ratio - sma20_ratio_multiply_upper_shadow_ratio.shift(2)
    sma20_ratio_multiply_upper_shadow_ratio_shift_3 = sma20_ratio_multiply_upper_shadow_ratio - sma20_ratio_multiply_upper_shadow_ratio.shift(3)

    # Feature: %b 表現價位於布林軌道中的位置加權
    bollinger_b5_multiply_upper_shadow_ratio = b5 * upper_shadow_ratio

    bollinger_b5_multiply_upper_shadow_ratio_shift_1 = bollinger_b5_multiply_upper_shadow_ratio - bollinger_b5_multiply_upper_shadow_ratio.shift(1)
    bollinger_b5_multiply_upper_shadow_ratio_shift_2 = bollinger_b5_multiply_upper_shadow_ratio - bollinger_b5_multiply_upper_shadow_ratio.shift(2)
    bollinger_b5_multiply_upper_shadow_ratio_shift_3 = bollinger_b5_multiply_upper_shadow_ratio - bollinger_b5_multiply_upper_shadow_ratio.shift(3)

    bollinger_b10_multiply_upper_shadow_ratio = b10 * upper_shadow_ratio

    bollinger_b10_multiply_upper_shadow_ratio_shift_1 = bollinger_b10_multiply_upper_shadow_ratio - bollinger_b10_multiply_upper_shadow_ratio.shift(1)
    bollinger_b10_multiply_upper_shadow_ratio_shift_2 = bollinger_b10_multiply_upper_shadow_ratio - bollinger_b10_multiply_upper_shadow_ratio.shift(2)
    bollinger_b10_multiply_upper_shadow_ratio_shift_3 = bollinger_b10_multiply_upper_shadow_ratio - bollinger_b10_multiply_upper_shadow_ratio.shift(3)

    bollinger_b22_multiply_upper_shadow_ratio = b22 * upper_shadow_ratio

    bollinger_b22_multiply_upper_shadow_ratio_shift_1 = bollinger_b22_multiply_upper_shadow_ratio - bollinger_b22_multiply_upper_shadow_ratio.shift(1)
    bollinger_b22_multiply_upper_shadow_ratio_shift_2 = bollinger_b22_multiply_upper_shadow_ratio - bollinger_b22_multiply_upper_shadow_ratio.shift(2)
    bollinger_b22_multiply_upper_shadow_ratio_shift_3 = bollinger_b22_multiply_upper_shadow_ratio - bollinger_b22_multiply_upper_shadow_ratio.shift(3)

  
    upper_shadow_ratio_volume_weighted = upper_shadow_ratio * lot_div_vma22
    # Feature: 上影線比例成交量加權與昨日、前二日、前三日的比值

    upper_shadow_ratio_volume_weighted_shift_1 = upper_shadow_ratio_volume_weighted - upper_shadow_ratio_volume_weighted.shift(1)
    upper_shadow_ratio_volume_weighted_shift_2 = upper_shadow_ratio_volume_weighted - upper_shadow_ratio_volume_weighted.shift(2)
    upper_shadow_ratio_volume_weighted_shift_3 = upper_shadow_ratio_volume_weighted - upper_shadow_ratio_volume_weighted.shift(3)

    # Feature: 五日均線分離率乘以上影線比例成交量加權
    sma5_ratio_multiply_upper_shadow_volume_weighted = (close / ema5) * upper_shadow_ratio_volume_weighted

    sma5_ratio_multiply_upper_shadow_volume_weighted_shift_1 = sma5_ratio_multiply_upper_shadow_volume_weighted - sma5_ratio_multiply_upper_shadow_volume_weighted.shift(1)
    sma5_ratio_multiply_upper_shadow_volume_weighted_shift_2 = sma5_ratio_multiply_upper_shadow_volume_weighted - sma5_ratio_multiply_upper_shadow_volume_weighted.shift(2)
    sma5_ratio_multiply_upper_shadow_volume_weighted_shift_3 = sma5_ratio_multiply_upper_shadow_volume_weighted - sma5_ratio_multiply_upper_shadow_volume_weighted.shift(3)

    # Feature: 十日均線分離率乘以上影線比例成交量加權
    sma10_ratio_multiply_upper_shadow_volume_weighted = (close / ema10) * upper_shadow_ratio_volume_weighted

    sma10_ratio_multiply_upper_shadow_volume_weighted_shift_1 = sma10_ratio_multiply_upper_shadow_volume_weighted - sma10_ratio_multiply_upper_shadow_volume_weighted.shift(1)
    sma10_ratio_multiply_upper_shadow_volume_weighted_shift_2 = sma10_ratio_multiply_upper_shadow_volume_weighted - sma10_ratio_multiply_upper_shadow_volume_weighted.shift(2)
    sma10_ratio_multiply_upper_shadow_volume_weighted_shift_3 = sma10_ratio_multiply_upper_shadow_volume_weighted - sma10_ratio_multiply_upper_shadow_volume_weighted.shift(3)

    # Feature: 二十日均線分離率乘以上影線比例成交量加權
    sma20_ratio_multiply_upper_shadow_volume_weighted = (close / ema22) * upper_shadow_ratio_volume_weighted

    sma20_ratio_multiply_upper_shadow_volume_weighted_shift_1 = sma20_ratio_multiply_upper_shadow_volume_weighted - sma20_ratio_multiply_upper_shadow_volume_weighted.shift(1)
    sma20_ratio_multiply_upper_shadow_volume_weighted_shift_2 = sma20_ratio_multiply_upper_shadow_volume_weighted - sma20_ratio_multiply_upper_shadow_volume_weighted.shift(2)
    sma20_ratio_multiply_upper_shadow_volume_weighted_shift_3 = sma20_ratio_multiply_upper_shadow_volume_weighted - sma20_ratio_multiply_upper_shadow_volume_weighted.shift(3)

    # Feature: %b 表現價位於布林軌道中的位置乘以上影線比例成交量加權
    bollinger_b5_multiply_upper_shadow_volume_weighted = b5 * upper_shadow_ratio_volume_weighted

    bollinger_b5_multiply_upper_shadow_volume_weighted_shift_1 = bollinger_b5_multiply_upper_shadow_volume_weighted - bollinger_b5_multiply_upper_shadow_volume_weighted.shift(1)
    bollinger_b5_multiply_upper_shadow_volume_weighted_shift_2 = bollinger_b5_multiply_upper_shadow_volume_weighted - bollinger_b5_multiply_upper_shadow_volume_weighted.shift(2)
    bollinger_b5_multiply_upper_shadow_volume_weighted_shift_3 = bollinger_b5_multiply_upper_shadow_volume_weighted - bollinger_b5_multiply_upper_shadow_volume_weighted.shift(3)

    bollinger_b10_multiply_upper_shadow_volume_weighted = b10 * upper_shadow_ratio_volume_weighted

    bollinger_b10_multiply_upper_shadow_volume_weighted_shift_1 = bollinger_b10_multiply_upper_shadow_volume_weighted - bollinger_b10_multiply_upper_shadow_volume_weighted.shift(1)
    bollinger_b10_multiply_upper_shadow_volume_weighted_shift_2 = bollinger_b10_multiply_upper_shadow_volume_weighted - bollinger_b10_multiply_upper_shadow_volume_weighted.shift(2)
    bollinger_b10_multiply_upper_shadow_volume_weighted_shift_3 = bollinger_b10_multiply_upper_shadow_volume_weighted - bollinger_b10_multiply_upper_shadow_volume_weighted.shift(3)

    bollinger_b22_multiply_upper_shadow_volume_weighted = b22 * upper_shadow_ratio_volume_weighted

    bollinger_b22_multiply_upper_shadow_volume_weighted_shift_1 = bollinger_b22_multiply_upper_shadow_volume_weighted - bollinger_b22_multiply_upper_shadow_volume_weighted.shift(1)
    bollinger_b22_multiply_upper_shadow_volume_weighted_shift_2 = bollinger_b22_multiply_upper_shadow_volume_weighted - bollinger_b22_multiply_upper_shadow_volume_weighted.shift(2)
    bollinger_b22_multiply_upper_shadow_volume_weighted_shift_3 = bollinger_b22_multiply_upper_shadow_volume_weighted - bollinger_b22_multiply_upper_shadow_volume_weighted.shift(3)

    # 計算下影線比例
    # 如果收盤價大於等於開盤價（收紅或收平），則下影線比例為 (開盤 – 最低) / 振幅
    # 如果開盤價大於收盤價（收黑），則下影線比例為 (收盤 – 最低) / 振幅
    lower_shadow_ratio = pd.Series(index=close.index)

    # 條件判斷
    red_or_flat = close >= open
    black = open > close

    # 計算下影線比例
    lower_shadow_ratio[red_or_flat] = (open - low)[red_or_flat] / amplitude[red_or_flat]
    lower_shadow_ratio[black] = (close - low)[black] / amplitude[black]

    # Feature: 下影線比例與昨日、前二日、前三日的比值

    lower_shadow_ratio_shift_1 = lower_shadow_ratio - lower_shadow_ratio.shift(1)
    lower_shadow_ratio_shift_2 = lower_shadow_ratio - lower_shadow_ratio.shift(2)
    lower_shadow_ratio_shift_3 = lower_shadow_ratio - lower_shadow_ratio.shift(3)

    
    # Feature: 五日均線分離率乘以下影線比例
    sma5_ratio_multiply_lower_shadow = (close / ema5) * lower_shadow_ratio

    sma5_ratio_multiply_lower_shadow_shift_1 = sma5_ratio_multiply_lower_shadow - sma5_ratio_multiply_lower_shadow.shift(1)
    sma5_ratio_multiply_lower_shadow_shift_2 = sma5_ratio_multiply_lower_shadow - sma5_ratio_multiply_lower_shadow.shift(2)
    sma5_ratio_multiply_lower_shadow_shift_3 = sma5_ratio_multiply_lower_shadow - sma5_ratio_multiply_lower_shadow.shift(3)

    # Feature: 十日均線分離率乘以下影線比例
    sma10_ratio_multiply_lower_shadow = (close / ema10) * lower_shadow_ratio

    sma10_ratio_multiply_lower_shadow_shift_1 = sma10_ratio_multiply_lower_shadow - sma10_ratio_multiply_lower_shadow.shift(1)
    sma10_ratio_multiply_lower_shadow_shift_2 = sma10_ratio_multiply_lower_shadow - sma10_ratio_multiply_lower_shadow.shift(2)
    sma10_ratio_multiply_lower_shadow_shift_3 = sma10_ratio_multiply_lower_shadow - sma10_ratio_multiply_lower_shadow.shift(3)

    # Feature: 二十日均線分離率乘以下影線比例
    sma20_ratio_multiply_lower_shadow = (close / ema22) * lower_shadow_ratio

    sma20_ratio_multiply_lower_shadow_shift_1 = sma20_ratio_multiply_lower_shadow - sma20_ratio_multiply_lower_shadow.shift(1)
    sma20_ratio_multiply_lower_shadow_shift_2 = sma20_ratio_multiply_lower_shadow - sma20_ratio_multiply_lower_shadow.shift(2)
    sma20_ratio_multiply_lower_shadow_shift_3 = sma20_ratio_multiply_lower_shadow - sma20_ratio_multiply_lower_shadow.shift(3)

    # Feature: %b 表現價位於布林軌道中的位置乘以下影線比例
    bollinger_b5_multiply_lower_shadow = b5 * lower_shadow_ratio

    bollinger_b5_multiply_lower_shadow_shift_1 = bollinger_b5_multiply_lower_shadow - bollinger_b5_multiply_lower_shadow.shift(1)
    bollinger_b5_multiply_lower_shadow_shift_2 = bollinger_b5_multiply_lower_shadow - bollinger_b5_multiply_lower_shadow.shift(2)
    bollinger_b5_multiply_lower_shadow_shift_3 = bollinger_b5_multiply_lower_shadow - bollinger_b5_multiply_lower_shadow.shift(3)

    bollinger_b10_multiply_lower_shadow = b10 * lower_shadow_ratio

    bollinger_b10_multiply_lower_shadow_shift_1 = bollinger_b10_multiply_lower_shadow - bollinger_b10_multiply_lower_shadow.shift(1)
    bollinger_b10_multiply_lower_shadow_shift_2 = bollinger_b10_multiply_lower_shadow - bollinger_b10_multiply_lower_shadow.shift(2)
    bollinger_b10_multiply_lower_shadow_shift_3 = bollinger_b10_multiply_lower_shadow - bollinger_b10_multiply_lower_shadow.shift(3)

    bollinger_b22_multiply_lower_shadow = b22 * lower_shadow_ratio

    bollinger_b22_multiply_lower_shadow_shift_1 = bollinger_b22_multiply_lower_shadow - bollinger_b22_multiply_lower_shadow.shift(1)
    bollinger_b22_multiply_lower_shadow_shift_2 = bollinger_b22_multiply_lower_shadow - bollinger_b22_multiply_lower_shadow.shift(2)
    bollinger_b22_multiply_lower_shadow_shift_3 = bollinger_b22_multiply_lower_shadow - bollinger_b22_multiply_lower_shadow.shift(3)

    lower_shadow_ratio_volume_weighted = lot_div_vma22 * lower_shadow_ratio
    
    # Feature: 下影線比例成交量加權與昨日、前二日、前三日的比值

    lower_shadow_ratio_volume_weighted_shift_1 = lower_shadow_ratio_volume_weighted - lower_shadow_ratio_volume_weighted.shift(1)
    lower_shadow_ratio_volume_weighted_shift_2 = lower_shadow_ratio_volume_weighted - lower_shadow_ratio_volume_weighted.shift(2)
    lower_shadow_ratio_volume_weighted_shift_3 = lower_shadow_ratio_volume_weighted - lower_shadow_ratio_volume_weighted.shift(3)

    # Feature: 五日均線分離率乘以下影線比例成交量加權
    sma5_ratio_multiply_lower_shadow_volume_weighted = (close / ema5) * lower_shadow_ratio_volume_weighted

    sma5_ratio_multiply_lower_shadow_volume_weighted_shift_1 = sma5_ratio_multiply_lower_shadow_volume_weighted - sma5_ratio_multiply_lower_shadow_volume_weighted.shift(1)
    sma5_ratio_multiply_lower_shadow_volume_weighted_shift_2 = sma5_ratio_multiply_lower_shadow_volume_weighted - sma5_ratio_multiply_lower_shadow_volume_weighted.shift(2)
    sma5_ratio_multiply_lower_shadow_volume_weighted_shift_3 = sma5_ratio_multiply_lower_shadow_volume_weighted - sma5_ratio_multiply_lower_shadow_volume_weighted.shift(3)

    # Feature: 十日均線分離率乘以下影線比例成交量加權
    sma10_ratio_multiply_lower_shadow_volume_weighted = (close / ema10) * lower_shadow_ratio_volume_weighted

    sma10_ratio_multiply_lower_shadow_volume_weighted_shift_1 = sma10_ratio_multiply_lower_shadow_volume_weighted - sma10_ratio_multiply_lower_shadow_volume_weighted.shift(1)
    sma10_ratio_multiply_lower_shadow_volume_weighted_shift_2 = sma10_ratio_multiply_lower_shadow_volume_weighted - sma10_ratio_multiply_lower_shadow_volume_weighted.shift(2)
    sma10_ratio_multiply_lower_shadow_volume_weighted_shift_3 = sma10_ratio_multiply_lower_shadow_volume_weighted - sma10_ratio_multiply_lower_shadow_volume_weighted.shift(3)

    # Feature: 二十日均線分離率乘以下影線比例成交量加權
    sma20_ratio_multiply_lower_shadow_volume_weighted = (close / ema22) * lower_shadow_ratio_volume_weighted

    sma20_ratio_multiply_lower_shadow_volume_weighted_shift_1 = sma20_ratio_multiply_lower_shadow_volume_weighted - sma20_ratio_multiply_lower_shadow_volume_weighted.shift(1)
    sma20_ratio_multiply_lower_shadow_volume_weighted_shift_2 = sma20_ratio_multiply_lower_shadow_volume_weighted - sma20_ratio_multiply_lower_shadow_volume_weighted.shift(2)
    sma20_ratio_multiply_lower_shadow_volume_weighted_shift_3 = sma20_ratio_multiply_lower_shadow_volume_weighted - sma20_ratio_multiply_lower_shadow_volume_weighted.shift(3)

    # Feature: %b5 表現價位於布林軌道中的位置乘以下影線比例成交量加權
    bollinger_b5_multiply_lower_shadow_volume_weighted = b5 * lower_shadow_ratio_volume_weighted

    bollinger_b5_multiply_lower_shadow_volume_weighted_shift_1 = bollinger_b5_multiply_lower_shadow_volume_weighted - bollinger_b5_multiply_lower_shadow_volume_weighted.shift(1)
    bollinger_b5_multiply_lower_shadow_volume_weighted_shift_2 = bollinger_b5_multiply_lower_shadow_volume_weighted - bollinger_b5_multiply_lower_shadow_volume_weighted.shift(2)
    bollinger_b5_multiply_lower_shadow_volume_weighted_shift_3 = bollinger_b5_multiply_lower_shadow_volume_weighted - bollinger_b5_multiply_lower_shadow_volume_weighted.shift(3)

    # Feature: %b10 表現價位於布林軌道中的位置乘以下影線比例成交量加權
    bollinger_b10_multiply_lower_shadow_volume_weighted = b10 * lower_shadow_ratio_volume_weighted

    bollinger_b10_multiply_lower_shadow_volume_weighted_shift_1 = bollinger_b10_multiply_lower_shadow_volume_weighted - bollinger_b10_multiply_lower_shadow_volume_weighted.shift(1)
    bollinger_b10_multiply_lower_shadow_volume_weighted_shift_2 = bollinger_b10_multiply_lower_shadow_volume_weighted - bollinger_b10_multiply_lower_shadow_volume_weighted.shift(2)
    bollinger_b10_multiply_lower_shadow_volume_weighted_shift_3 = bollinger_b10_multiply_lower_shadow_volume_weighted - bollinger_b10_multiply_lower_shadow_volume_weighted.shift(3)

    # Feature: %b22 表現價位於布林軌道中的位置乘以下影線比例成交量加權
    bollinger_b22_multiply_lower_shadow_volume_weighted = b22 * lower_shadow_ratio_volume_weighted

    bollinger_b22_multiply_lower_shadow_volume_weighted_shift_1 = bollinger_b22_multiply_lower_shadow_volume_weighted - bollinger_b22_multiply_lower_shadow_volume_weighted.shift(1)
    bollinger_b22_multiply_lower_shadow_volume_weighted_shift_2 = bollinger_b22_multiply_lower_shadow_volume_weighted - bollinger_b22_multiply_lower_shadow_volume_weighted.shift(2)
    bollinger_b22_multiply_lower_shadow_volume_weighted_shift_3 = bollinger_b22_multiply_lower_shadow_volume_weighted - bollinger_b22_multiply_lower_shadow_volume_weighted.shift(3)

    # 設定 a 的值
    a = 0.5  # 你可以根據需求調整這個值，範圍在 0.5 到 0.75 之間

    # 計算整體K棒強弱
    overall_k_strength = real_ratio + a * (upper_shadow_ratio + lower_shadow_ratio)

    # Feature: 整體K棒強弱與昨日、前二日、前三日的比值

    overall_k_strength_shift_1 = overall_k_strength - overall_k_strength.shift(1)
    overall_k_strength_shift_2 = overall_k_strength - overall_k_strength.shift(2)
    overall_k_strength_shift_3 = overall_k_strength - overall_k_strength.shift(3)

    # Feature: 整體K棒強弱的移動平均線
    sma5_overall_k_strength = overall_k_strength.rolling(window=5).mean()
    sma10_overall_k_strength = overall_k_strength.rolling(window=10).mean()
    sma20_overall_k_strength = overall_k_strength.rolling(window=20).mean()

    # Feature: 整體K棒強弱與移動平均線的比值

    overall_k_strength_div_sma5 = overall_k_strength - sma5_overall_k_strength
    overall_k_strength_div_sma10 = overall_k_strength - sma10_overall_k_strength
    overall_k_strength_div_sma20 = overall_k_strength - sma20_overall_k_strength


    # Feature: 不同移動平均線之間的比值
 
    sma5_div_sma10_overall_k_strength = sma5_overall_k_strength - sma10_overall_k_strength
    sma5_div_sma20_overall_k_strength = sma5_overall_k_strength - sma20_overall_k_strength
    sma10_div_sma20_overall_k_strength = sma10_overall_k_strength - sma20_overall_k_strength

    # 計算均線角度（angle）
    angle_sma5_overall_k_strength = np.degrees(np.arctan(sma5_overall_k_strength.diff()))
    angle_sma10_overall_k_strength = np.degrees(np.arctan(sma10_overall_k_strength.diff()))
    angle_sma20_overall_k_strength = np.degrees(np.arctan(sma20_overall_k_strength.diff()))
    
    # Feature: 五日均線分離率乘以整體K棒強弱
    sma5_ratio_multiply_overall_k_strength = (close / ema5) * overall_k_strength

    sma5_ratio_multiply_overall_k_strength_shift_1 = sma5_ratio_multiply_overall_k_strength - sma5_ratio_multiply_overall_k_strength.shift(1)
    sma5_ratio_multiply_overall_k_strength_shift_2 = sma5_ratio_multiply_overall_k_strength - sma5_ratio_multiply_overall_k_strength.shift(2)
    sma5_ratio_multiply_overall_k_strength_shift_3 = sma5_ratio_multiply_overall_k_strength - sma5_ratio_multiply_overall_k_strength.shift(3)

    # Feature: 十日均線分離率乘以整體K棒強弱
    sma10_ratio_multiply_overall_k_strength = (close / ema10) * overall_k_strength

    sma10_ratio_multiply_overall_k_strength_shift_1 = sma10_ratio_multiply_overall_k_strength - sma10_ratio_multiply_overall_k_strength.shift(1)
    sma10_ratio_multiply_overall_k_strength_shift_2 = sma10_ratio_multiply_overall_k_strength - sma10_ratio_multiply_overall_k_strength.shift(2)
    sma10_ratio_multiply_overall_k_strength_shift_3 = sma10_ratio_multiply_overall_k_strength - sma10_ratio_multiply_overall_k_strength.shift(3)

    # Feature: 二十日均線分離率乘以整體K棒強弱
    sma20_ratio_multiply_overall_k_strength = (close / ema22) * overall_k_strength

    sma20_ratio_multiply_overall_k_strength_shift_1 = sma20_ratio_multiply_overall_k_strength - sma20_ratio_multiply_overall_k_strength.shift(1)
    sma20_ratio_multiply_overall_k_strength_shift_2 = sma20_ratio_multiply_overall_k_strength - sma20_ratio_multiply_overall_k_strength.shift(2)
    sma20_ratio_multiply_overall_k_strength_shift_3 = sma20_ratio_multiply_overall_k_strength - sma20_ratio_multiply_overall_k_strength.shift(3)

    # Feature: %b5 表現價位於布林軌道中的位置乘以整體K棒強弱
    bollinger_b5_multiply_overall_k_strength = b5 * overall_k_strength

    bollinger_b5_multiply_overall_k_strength_shift_1 = bollinger_b5_multiply_overall_k_strength - bollinger_b5_multiply_overall_k_strength.shift(1)
    bollinger_b5_multiply_overall_k_strength_shift_2 = bollinger_b5_multiply_overall_k_strength - bollinger_b5_multiply_overall_k_strength.shift(2)
    bollinger_b5_multiply_overall_k_strength_shift_3 = bollinger_b5_multiply_overall_k_strength - bollinger_b5_multiply_overall_k_strength.shift(3)

    # Feature: %b10 表現價位於布林軌道中的位置乘以整體K棒強弱
    bollinger_b10_multiply_overall_k_strength = b10 * overall_k_strength

    bollinger_b10_multiply_overall_k_strength_shift_1 = bollinger_b10_multiply_overall_k_strength - bollinger_b10_multiply_overall_k_strength.shift(1)
    bollinger_b10_multiply_overall_k_strength_shift_2 = bollinger_b10_multiply_overall_k_strength - bollinger_b10_multiply_overall_k_strength.shift(2)
    bollinger_b10_multiply_overall_k_strength_shift_3 = bollinger_b10_multiply_overall_k_strength - bollinger_b10_multiply_overall_k_strength.shift(3)

    # Feature: %b22 表現價位於布林軌道中的位置乘以整體K棒強弱
    bollinger_b22_multiply_overall_k_strength = b22 * overall_k_strength

    bollinger_b22_multiply_overall_k_strength_shift_1 = bollinger_b22_multiply_overall_k_strength - bollinger_b22_multiply_overall_k_strength.shift(1)
    bollinger_b22_multiply_overall_k_strength_shift_2 = bollinger_b22_multiply_overall_k_strength - bollinger_b22_multiply_overall_k_strength.shift(2)
    bollinger_b22_multiply_overall_k_strength_shift_3 = bollinger_b22_multiply_overall_k_strength - bollinger_b22_multiply_overall_k_strength.shift(3)

    # 計算整體K棒強弱成交量加權
    overall_k_strength_volume_weighted = real_ratio_volume_weighted + a * (upper_shadow_ratio_volume_weighted + lower_shadow_ratio_volume_weighted)

    # Feature: 整體K棒強弱成交量加權與昨日、前二日、前三日的比值

    overall_k_strength_volume_weighted_shift_1 = overall_k_strength_volume_weighted - overall_k_strength_volume_weighted.shift(1)
    overall_k_strength_volume_weighted_shift_2 = overall_k_strength_volume_weighted - overall_k_strength_volume_weighted.shift(2)
    overall_k_strength_volume_weighted_shift_3 = overall_k_strength_volume_weighted - overall_k_strength_volume_weighted.shift(3)

    # Feature: 整體K棒強弱成交量加權的移動平均線
    sma5_overall_k_strength_volume_weighted = overall_k_strength_volume_weighted.rolling(window=5).mean()
    sma10_overall_k_strength_volume_weighted = overall_k_strength_volume_weighted.rolling(window=10).mean()
    sma20_overall_k_strength_volume_weighted = overall_k_strength_volume_weighted.rolling(window=20).mean()

    # Feature: 整體K棒強弱成交量加權與移動平均線的比值

    overall_k_strength_volume_weighted_div_sma5 = overall_k_strength_volume_weighted - sma5_overall_k_strength_volume_weighted
    overall_k_strength_volume_weighted_div_sma10 = overall_k_strength_volume_weighted - sma10_overall_k_strength_volume_weighted
    overall_k_strength_volume_weighted_div_sma20 = overall_k_strength_volume_weighted - sma20_overall_k_strength_volume_weighted

    # Feature: 不同移動平均線之間的比值

    sma5_div_sma10_overall_k_strength_volume_weighted = sma5_overall_k_strength_volume_weighted - sma10_overall_k_strength_volume_weighted
    sma5_div_sma20_overall_k_strength_volume_weighted = sma5_overall_k_strength_volume_weighted - sma20_overall_k_strength_volume_weighted
    sma10_div_sma20_overall_k_strength_volume_weighted = sma10_overall_k_strength_volume_weighted - sma20_overall_k_strength_volume_weighted

    # 計算均線角度（angle）
    angle_sma5_overall_k_strength_volume_weighted = np.degrees(np.arctan(sma5_overall_k_strength_volume_weighted.diff()))
    angle_sma10_overall_k_strength_volume_weighted = np.degrees(np.arctan(sma10_overall_k_strength_volume_weighted.diff()))
    angle_sma20_overall_k_strength_volume_weighted = np.degrees(np.arctan(sma20_overall_k_strength_volume_weighted.diff()))
    
    # Feature: 五日均線分離率乘以整體K棒強弱成交量加權
    sma5_ratio_multiply_overall_k_strength_volume_weighted = (close / ema5) * overall_k_strength_volume_weighted

    sma5_ratio_multiply_overall_k_strength_volume_weighted_shift_1 = sma5_ratio_multiply_overall_k_strength_volume_weighted - sma5_ratio_multiply_overall_k_strength_volume_weighted.shift(1)
    sma5_ratio_multiply_overall_k_strength_volume_weighted_shift_2 = sma5_ratio_multiply_overall_k_strength_volume_weighted - sma5_ratio_multiply_overall_k_strength_volume_weighted.shift(2)
    sma5_ratio_multiply_overall_k_strength_volume_weighted_shift_3 = sma5_ratio_multiply_overall_k_strength_volume_weighted - sma5_ratio_multiply_overall_k_strength_volume_weighted.shift(3)

    # Feature: 十日均線分離率乘以整體K棒強弱成交量加權
    sma10_ratio_multiply_overall_k_strength_volume_weighted = (close / ema10) * overall_k_strength_volume_weighted

    sma10_ratio_multiply_overall_k_strength_volume_weighted_shift_1 = sma10_ratio_multiply_overall_k_strength_volume_weighted - sma10_ratio_multiply_overall_k_strength_volume_weighted.shift(1)
    sma10_ratio_multiply_overall_k_strength_volume_weighted_shift_2 = sma10_ratio_multiply_overall_k_strength_volume_weighted - sma10_ratio_multiply_overall_k_strength_volume_weighted.shift(2)
    sma10_ratio_multiply_overall_k_strength_volume_weighted_shift_3 = sma10_ratio_multiply_overall_k_strength_volume_weighted - sma10_ratio_multiply_overall_k_strength_volume_weighted.shift(3)

    # Feature: 二十日均線分離率乘以整體K棒強弱成交量加權
    sma20_ratio_multiply_overall_k_strength_volume_weighted = (close / ema22) * overall_k_strength_volume_weighted

    sma20_ratio_multiply_overall_k_strength_volume_weighted_shift_1 = sma20_ratio_multiply_overall_k_strength_volume_weighted - sma20_ratio_multiply_overall_k_strength_volume_weighted.shift(1)
    sma20_ratio_multiply_overall_k_strength_volume_weighted_shift_2 = sma20_ratio_multiply_overall_k_strength_volume_weighted - sma20_ratio_multiply_overall_k_strength_volume_weighted.shift(2)
    sma20_ratio_multiply_overall_k_strength_volume_weighted_shift_3 = sma20_ratio_multiply_overall_k_strength_volume_weighted - sma20_ratio_multiply_overall_k_strength_volume_weighted.shift(3)

    # Feature: %b5 表現價位於布林軌道中的位置乘以整體K棒強弱成交量加權
    bollinger_b5_multiply_overall_k_strength_volume_weighted = b5 * overall_k_strength_volume_weighted

    bollinger_b5_multiply_overall_k_strength_volume_weighted_shift_1 = bollinger_b5_multiply_overall_k_strength_volume_weighted - bollinger_b5_multiply_overall_k_strength_volume_weighted.shift(1)
    bollinger_b5_multiply_overall_k_strength_volume_weighted_shift_2 = bollinger_b5_multiply_overall_k_strength_volume_weighted - bollinger_b5_multiply_overall_k_strength_volume_weighted.shift(2)
    bollinger_b5_multiply_overall_k_strength_volume_weighted_shift_3 = bollinger_b5_multiply_overall_k_strength_volume_weighted - bollinger_b5_multiply_overall_k_strength_volume_weighted.shift(3)

    # Feature: %b10 表現價位於布林軌道中的位置乘以整體K棒強弱成交量加權
    bollinger_b10_multiply_overall_k_strength_volume_weighted = b10 * overall_k_strength_volume_weighted

    bollinger_b10_multiply_overall_k_strength_volume_weighted_shift_1 = bollinger_b10_multiply_overall_k_strength_volume_weighted - bollinger_b10_multiply_overall_k_strength_volume_weighted.shift(1)
    bollinger_b10_multiply_overall_k_strength_volume_weighted_shift_2 = bollinger_b10_multiply_overall_k_strength_volume_weighted - bollinger_b10_multiply_overall_k_strength_volume_weighted.shift(2)
    bollinger_b10_multiply_overall_k_strength_volume_weighted_shift_3 = bollinger_b10_multiply_overall_k_strength_volume_weighted - bollinger_b10_multiply_overall_k_strength_volume_weighted.shift(3)

    # Feature: %b22 表現價位於布林軌道中的位置乘以整體K棒強弱成交量加權
    bollinger_b22_multiply_overall_k_strength_volume_weighted = b22 * overall_k_strength_volume_weighted

    bollinger_b22_multiply_overall_k_strength_volume_weighted_shift_1 = bollinger_b22_multiply_overall_k_strength_volume_weighted - bollinger_b22_multiply_overall_k_strength_volume_weighted.shift(1)
    bollinger_b22_multiply_overall_k_strength_volume_weighted_shift_2 = bollinger_b22_multiply_overall_k_strength_volume_weighted - bollinger_b22_multiply_overall_k_strength_volume_weighted.shift(2)
    bollinger_b22_multiply_overall_k_strength_volume_weighted_shift_3 = bollinger_b22_multiply_overall_k_strength_volume_weighted - bollinger_b22_multiply_overall_k_strength_volume_weighted.shift(3)

    # 計算影線比例
    shadow_ratio = lower_shadow_ratio - upper_shadow_ratio

    # Feature: 影線比例與前幾日的比值

    shadow_ratio_shift_1 = shadow_ratio - shadow_ratio.shift(1)
    shadow_ratio_shift_2 = shadow_ratio - shadow_ratio.shift(2)
    shadow_ratio_shift_3 = shadow_ratio - shadow_ratio.shift(3)


    # 計算影線比例成交量加權
    shadow_ratio_volume_weighted = lower_shadow_ratio_volume_weighted - upper_shadow_ratio_volume_weighted

    # Feature: 影線比例成交量加權與前幾日的比值

    shadow_ratio_volume_weighted_shift_1 = shadow_ratio_volume_weighted - shadow_ratio_volume_weighted.shift(1)
    shadow_ratio_volume_weighted_shift_2 = shadow_ratio_volume_weighted - shadow_ratio_volume_weighted.shift(2)
    shadow_ratio_volume_weighted_shift_3 = shadow_ratio_volume_weighted - shadow_ratio_volume_weighted.shift(3)

    # 計算影線差異比
    shadow_difference_ratio = upper_shadow_ratio + lower_shadow_ratio

    # Feature: 影線差異比與前幾日的比值

    shadow_difference_ratio_shift_1 = shadow_difference_ratio - shadow_difference_ratio.shift(1)
    shadow_difference_ratio_shift_2 = shadow_difference_ratio - shadow_difference_ratio.shift(2)
    shadow_difference_ratio_shift_3 = shadow_difference_ratio - shadow_difference_ratio.shift(3)

    # 計算影線差異成交量加權
    shadow_difference_volume_weighted = upper_shadow_ratio_volume_weighted + lower_shadow_ratio_volume_weighted

    # Feature: 修正加權影線差與前幾日的比值

    shadow_difference_volume_weighted_shift_1 = shadow_difference_volume_weighted - shadow_difference_volume_weighted.shift(1)
    shadow_difference_volume_weighted_shift_2 = shadow_difference_volume_weighted - shadow_difference_volume_weighted.shift(2)
    shadow_difference_volume_weighted_shift_3 = shadow_difference_volume_weighted - shadow_difference_volume_weighted.shift(3)

    # 設定常數值
    k = 0.9
    epsilon = 1e-6  # 用於避免分母或分子為零的極小值

    # 計算修正實體影線比
    adjusted_real_shadow_ratio = (real_ratio + k * epsilon) / (shadow_ratio + (1 - k) * epsilon)

    # Feature: 修正實體影線比與前幾日的比值

    adjusted_real_shadow_ratio_shift_1 = adjusted_real_shadow_ratio - adjusted_real_shadow_ratio.shift(1)
    adjusted_real_shadow_ratio_shift_2 = adjusted_real_shadow_ratio - adjusted_real_shadow_ratio.shift(2)
    adjusted_real_shadow_ratio_shift_3 = adjusted_real_shadow_ratio - adjusted_real_shadow_ratio.shift(3)

    
    # 計算開盤比例
    open_ratio = (open - close.shift(1)) / amplitude

    # 修改後的 Feature: 開盤比例與前幾日的比值

    open_ratio_shift_1 = open_ratio - open_ratio.shift(1)
    open_ratio_shift_2 = open_ratio - open_ratio.shift(2)
    open_ratio_shift_3 = open_ratio - open_ratio.shift(3)


    # Feature: 開盤比例的移動平均線
    sma5_open_ratio = open_ratio.rolling(window=5).mean()
    sma10_open_ratio = open_ratio.rolling(window=10).mean()
    sma20_open_ratio = open_ratio.rolling(window=20).mean()

    # Feature: 開盤比例與移動平均線的比值

    open_ratio_div_sma5 = open_ratio - sma5_open_ratio
    open_ratio_div_sma10 = open_ratio - sma10_open_ratio
    open_ratio_div_sma20 = open_ratio - sma20_open_ratio

    # Feature: 不同移動平均線之間的比值

    sma5_div_sma10_open_ratio = sma5_open_ratio - sma10_open_ratio
    sma5_div_sma20_open_ratio = sma5_open_ratio - sma20_open_ratio
    sma10_div_sma20_open_ratio = sma10_open_ratio - sma20_open_ratio

    # 計算收盤比例
    close_ratio = (close - close.shift(1)) / amplitude

    # Feature: 收盤比例與前幾日的比值

    close_ratio_shift_1 = close_ratio - close_ratio.shift(1)
    close_ratio_shift_2 = close_ratio - close_ratio.shift(2)
    close_ratio_shift_3 = close_ratio - close_ratio.shift(3)

    # Feature: 收盤比例的移動平均線
    sma5_close_ratio = close_ratio.rolling(window=5).mean()
    sma10_close_ratio = close_ratio.rolling(window=10).mean()
    sma20_close_ratio = close_ratio.rolling(window=20).mean()

    # Feature: 收盤比例與移動平均線的比值

    close_ratio_div_sma5 = close_ratio - sma5_close_ratio
    close_ratio_div_sma10 = close_ratio - sma10_close_ratio
    close_ratio_div_sma20 = close_ratio - sma20_close_ratio

    # Feature: 不同移動平均線之間的比值

    sma5_div_sma10_close_ratio = sma5_close_ratio - sma10_close_ratio
    sma5_div_sma20_close_ratio = sma5_close_ratio - sma20_close_ratio
    sma10_div_sma20_close_ratio = sma10_close_ratio - sma20_close_ratio

    # 計算最高比例
    high_ratio = (high - close.shift(1)) / amplitude

    # Feature: 最高比例與前幾日的比值

    high_ratio_shift_1 = high_ratio - high_ratio.shift(1)
    high_ratio_shift_2 = high_ratio - high_ratio.shift(2)
    high_ratio_shift_3 = high_ratio - high_ratio.shift(3)

    # Feature: 最高比例的移動平均線
    sma5_high_ratio = high_ratio.rolling(window=5).mean()
    sma10_high_ratio = high_ratio.rolling(window=10).mean()
    sma20_high_ratio = high_ratio.rolling(window=20).mean()

    # Feature: 最高比例與移動平均線的比值

    high_ratio_div_sma5 = high_ratio - sma5_high_ratio
    high_ratio_div_sma10 = high_ratio - sma10_high_ratio
    high_ratio_div_sma20 = high_ratio - sma20_high_ratio

    # Feature: 不同移動平均線之間的比值

    sma5_div_sma10_high_ratio = sma5_high_ratio - sma10_high_ratio
    sma5_div_sma20_high_ratio = sma5_high_ratio - sma20_high_ratio
    sma10_div_sma20_high_ratio = sma10_high_ratio - sma20_high_ratio

    # 計算最低比例
    low_ratio = (low - close.shift(1)) / amplitude

    # Feature: 最低比例與前幾日的比值

    low_ratio_shift_1 = low_ratio - low_ratio.shift(1)
    low_ratio_shift_2 = low_ratio - low_ratio.shift(2)
    low_ratio_shift_3 = low_ratio - low_ratio.shift(3)

    # Feature: 最低比例的移動平均線
    sma5_low_ratio = low_ratio.rolling(window=5).mean()
    sma10_low_ratio = low_ratio.rolling(window=10).mean()
    sma20_low_ratio = low_ratio.rolling(window=20).mean()

    # Feature: 最低比例與移動平均線的比值

    low_ratio_div_sma5 = low_ratio - sma5_low_ratio
    low_ratio_div_sma10 = low_ratio - sma10_low_ratio
    low_ratio_div_sma20 = low_ratio - sma20_low_ratio

    # Feature: 不同移動平均線之間的比值

    sma5_div_sma10_low_ratio = sma5_low_ratio - sma10_low_ratio
    sma5_div_sma20_low_ratio = sma5_low_ratio - sma20_low_ratio
    sma10_div_sma20_low_ratio = sma10_low_ratio - sma20_low_ratio

    # 計算高開比例
    high_open_ratio = (high - open) / amplitude

    # Feature: 高開比例與前幾日的比值

    high_open_ratio_shift_1 = high_open_ratio - high_open_ratio.shift(1)
    high_open_ratio_shift_2 = high_open_ratio - high_open_ratio.shift(2)
    high_open_ratio_shift_3 = high_open_ratio - high_open_ratio.shift(3)

    # Feature: 高開比例的移動平均線
    sma5_high_open_ratio = high_open_ratio.rolling(window=5).mean()
    sma10_high_open_ratio = high_open_ratio.rolling(window=10).mean()
    sma20_high_open_ratio = high_open_ratio.rolling(window=20).mean()

    # Feature: 高開比例與移動平均線的比值

    high_open_ratio_div_sma5 = high_open_ratio - sma5_high_open_ratio
    high_open_ratio_div_sma10 = high_open_ratio - sma10_high_open_ratio
    high_open_ratio_div_sma20 = high_open_ratio - sma20_high_open_ratio


    # Feature: 不同移動平均線之間的比值

    sma5_div_sma10_high_open_ratio = sma5_high_open_ratio - sma10_high_open_ratio
    sma5_div_sma20_high_open_ratio = sma5_high_open_ratio - sma20_high_open_ratio
    sma10_div_sma20_high_open_ratio = sma10_high_open_ratio - sma20_high_open_ratio

    # 計算低開比例
    low_open_ratio = (low - open) / amplitude

    # Feature: 低開比例與前幾日的比值

    low_open_ratio_shift_1 = low_open_ratio - low_open_ratio.shift(1)
    low_open_ratio_shift_2 = low_open_ratio - low_open_ratio.shift(2)
    low_open_ratio_shift_3 = low_open_ratio - low_open_ratio.shift(3)

    # Feature: 低開比例的移動平均線
    sma5_low_open_ratio = low_open_ratio.rolling(window=5).mean()
    sma10_low_open_ratio = low_open_ratio.rolling(window=10).mean()
    sma20_low_open_ratio = low_open_ratio.rolling(window=20).mean()

    # Feature: 低開比例與移動平均線的比值

    low_open_ratio_div_sma5 = low_open_ratio - sma5_low_open_ratio
    low_open_ratio_div_sma10 = low_open_ratio - sma10_low_open_ratio
    low_open_ratio_div_sma20 = low_open_ratio - sma20_low_open_ratio

    # Feature: 不同移動平均線之間的比值

    sma5_div_sma10_low_open_ratio = sma5_low_open_ratio - sma10_low_open_ratio
    sma5_div_sma20_low_open_ratio = sma5_low_open_ratio - sma20_low_open_ratio
    sma10_div_sma20_low_open_ratio = sma10_low_open_ratio - sma20_low_open_ratio

    
    # 計算高收比例
    high_close_ratio = (high - close) / amplitude

    # Feature: 高收比例與前幾日的比值

    high_close_ratio_shift_1 = high_close_ratio - high_close_ratio.shift(1)
    high_close_ratio_shift_2 = high_close_ratio - high_close_ratio.shift(2)
    high_close_ratio_shift_3 = high_close_ratio - high_close_ratio.shift(3)

    # Feature: 高收比例的移動平均線
    sma5_high_close_ratio = high_close_ratio.rolling(window=5).mean()
    sma10_high_close_ratio = high_close_ratio.rolling(window=10).mean()
    sma20_high_close_ratio = high_close_ratio.rolling(window=20).mean()

    # Feature: 高收比例與移動平均線的比值
    
    high_close_ratio_div_sma5 = high_close_ratio - sma5_high_close_ratio
    high_close_ratio_div_sma10 = high_close_ratio - sma10_high_close_ratio
    high_close_ratio_div_sma20 = high_close_ratio - sma20_high_close_ratio

    # Feature: 不同移動平均線之間的比值

    sma5_div_sma10_high_close_ratio = sma5_high_close_ratio - sma10_high_close_ratio
    sma5_div_sma20_high_close_ratio = sma5_high_close_ratio - sma20_high_close_ratio
    sma10_div_sma20_high_close_ratio = sma10_high_close_ratio - sma20_high_close_ratio

    # 計算低收比例
    low_close_ratio = (low - close) / amplitude

    # Feature: 低收比例與前幾日的比值

    low_close_ratio_shift_1 = low_close_ratio - low_close_ratio.shift(1)
    low_close_ratio_shift_2 = low_close_ratio - low_close_ratio.shift(2)
    low_close_ratio_shift_3 = low_close_ratio - low_close_ratio.shift(3)

    # Feature: 低收比例的移動平均線
    sma5_low_close_ratio = low_close_ratio.rolling(window=5).mean()
    sma10_low_close_ratio = low_close_ratio.rolling(window=10).mean()
    sma20_low_close_ratio = low_close_ratio.rolling(window=20).mean()

    # Feature: 低收比例與移動平均線的比值

    low_close_ratio_div_sma5 = low_close_ratio - sma5_low_close_ratio
    low_close_ratio_div_sma10 = low_close_ratio - sma10_low_close_ratio
    low_close_ratio_div_sma20 = low_close_ratio - sma20_low_close_ratio

    # Feature: 不同移動平均線之間的比值

    sma5_div_sma10_low_close_ratio = sma5_low_close_ratio - sma10_low_close_ratio
    sma5_div_sma20_low_close_ratio = sma5_low_close_ratio - sma20_low_close_ratio
    sma10_div_sma20_low_close_ratio = sma10_low_close_ratio - sma20_low_close_ratio

    # 計算高收低收比
    high_low_close_ratio = ((high - close) - (low - close)) / amplitude

    # Feature: 高收低收比與前幾日的比值

    high_low_close_ratio_shift_1 = high_low_close_ratio - high_low_close_ratio.shift(1)
    high_low_close_ratio_shift_2 = high_low_close_ratio - high_low_close_ratio.shift(2)
    high_low_close_ratio_shift_3 = high_low_close_ratio - high_low_close_ratio.shift(3)

    # 計算高收低收比成交量加權
    high_low_close_ratio_volume_weighted = (lot_div_vma22 * ((high - close) - (low - close))) / amplitude

    # Feature: 高收低收比成交量加權與前幾日的比值

    high_low_close_ratio_volume_weighted_shift_1 = high_low_close_ratio_volume_weighted - high_low_close_ratio_volume_weighted.shift(1)
    high_low_close_ratio_volume_weighted_shift_2 = high_low_close_ratio_volume_weighted - high_low_close_ratio_volume_weighted.shift(2)
    high_low_close_ratio_volume_weighted_shift_3 = high_low_close_ratio_volume_weighted - high_low_close_ratio_volume_weighted.shift(3)

    # 計算上影線與實體K棒強弱
    upper_shadow_and_real_k_strength = real_ratio + 0.5 * upper_shadow_ratio

    # Feature: 上影線與實體K棒強弱與前幾日的比值

    upper_shadow_and_real_k_strength_shift_1 = upper_shadow_and_real_k_strength - upper_shadow_and_real_k_strength.shift(1)
    upper_shadow_and_real_k_strength_shift_2 = upper_shadow_and_real_k_strength - upper_shadow_and_real_k_strength.shift(2)
    upper_shadow_and_real_k_strength_shift_3 = upper_shadow_and_real_k_strength - upper_shadow_and_real_k_strength.shift(3)

    # 計算下影線與實體K棒強弱
    lower_shadow_and_real_k_strength = real_ratio + 0.5 * lower_shadow_ratio

    # Feature: 下影線與實體K棒強弱與前幾日的比值

    lower_shadow_and_real_k_strength_shift_1 = lower_shadow_and_real_k_strength - lower_shadow_and_real_k_strength.shift(1)
    lower_shadow_and_real_k_strength_shift_2 = lower_shadow_and_real_k_strength - lower_shadow_and_real_k_strength.shift(2)
    lower_shadow_and_real_k_strength_shift_3 = lower_shadow_and_real_k_strength - lower_shadow_and_real_k_strength.shift(3)

    # 計算開盤與高低比
    # 先處理一般情況
    open_high_low_ratio = ind.map_to_minus_one_to_one(open / (high - low))

    # 處理特殊情況
    condition_limit_up = (high == low) & (open == high) & (open > close.shift(1))
    condition_limit_down = (high == low) & (open == low) & (open < close.shift(1))

    # 對特殊情況進行賦值
    open_high_low_ratio[condition_limit_up] = 1
    open_high_low_ratio[condition_limit_down] = 0

    # Feature: 開盤與高低比與前幾日的比值

    open_high_low_ratio_shift_1 = open_high_low_ratio - open_high_low_ratio.shift(1)
    open_high_low_ratio_shift_2 = open_high_low_ratio - open_high_low_ratio.shift(2)
    open_high_low_ratio_shift_3 = open_high_low_ratio - open_high_low_ratio.shift(3)

    # 計算收盤與高低比
    # 先處理一般情況
    close_high_low_ratio = ind.map_to_minus_one_to_one(close / (high - low))

    # 處理特殊情況
    condition_limit_up = (high == low) & (open == high) & (open > close.shift(1))
    condition_limit_down = (high == low) & (open == low) & (open < close.shift(1))

    # 對特殊情況進行賦值，使用 .loc[] 確保修改的是原始數據
    close_high_low_ratio[condition_limit_up] = 1
    close_high_low_ratio[condition_limit_down] = 0

    # Feature: 收盤與高低比與前幾日的比值    
    close_high_low_ratio_shift_1 = close_high_low_ratio - close_high_low_ratio.shift(1)
    close_high_low_ratio_shift_2 = close_high_low_ratio - close_high_low_ratio.shift(2)
    close_high_low_ratio_shift_3 = close_high_low_ratio - close_high_low_ratio.shift(3)

    # 計算累計系數 a, b, c
    ha = 1.5 if close[0] >= high[1] else 1
    hb = 1.5 if close[0] >= high[2] else 1
    hc = 1.5 if close[0] >= high[3] else 1

    # 計算收盤與近日最高比累計
    cumulative_ratio = ha * (close[0] / high[1]) + hb * (close[0] / high[2]) + hc * (close[0] / high[3])
    
    # 計算累計系數 a, b, c
    la = 1.5 if close[0] >= low[1] else 1
    lb = 1.5 if close[0] >= low[2] else 1
    lc = 1.5 if close[0] >= low[3] else 1

    # 計算收盤與近日最低比累計
    cumulative_low_ratio = la * (close[0] / low[1]) + lb * (close[0] / low[2]) + lc * (close[0] / low[3])
    
    # 計算三日最低比較
    three_day_low_comparison = (low[0] / low[1]) + (low[1] / low[2]) + (low[2] / low[3])

    # 計算五日最低比較
    five_day_low_comparison = (low[0] / low[1]) + (low[1] / low[2]) + (low[2] / low[3]) + (low[3] / low[4]) + (low[4] / low[5])
    
    # 計算三日最高比較
    three_day_high_comparison = (high[0] / high[1]) + (high[1] / high[2]) + (high[2] / high[3])

    # 計算五日最高比較
    five_day_high_comparison = (high[0] / high[1]) + (high[1] / high[2]) + (high[2] / high[3]) + (high[3] / high[4]) + (high[4] / high[5])
    
    # 計算三日收綠比例
    three_day_negative_real_ratio = real_ratio[-3:][real_ratio[-3:] < 0].sum()

    # 計算五日收綠比例
    five_day_negative_real_ratio = real_ratio[-5:][real_ratio[-5:] < 0].sum()

    # 計算十日收綠比例
    ten_day_negative_real_ratio = real_ratio[-10:][real_ratio[-10:] < 0].sum()

    # 計算三日收綠比例與十日收綠比例的比值
    three_day_div_ten_day_negative_real_ratio = three_day_negative_real_ratio - ten_day_negative_real_ratio
    
    # 計算三日收紅比例
    three_day_positive_real_ratio = real_ratio[-3:][real_ratio[-3:] > 0].sum()
    # 計算五日收紅比例
    five_day_positive_real_ratio = real_ratio[-5:][real_ratio[-5:] > 0].sum()
    # 計算十日收紅比例
    ten_day_positive_real_ratio = real_ratio[-10:][real_ratio[-10:] > 0].sum()

    # 計算三日收紅比例與十日收紅比例的比值
    three_day_div_ten_day_positive_real_ratio = three_day_positive_real_ratio - ten_day_positive_real_ratio

    # EMA 指標
    ema_dict = {
        'ema5': ema5,
        'ema10': ema10,
        'ema22': ema22,
        'ema66': ema66,
        'ema132': ema132,
        'ema264': ema264
    }
    
    # bollinger
    bollinger_bands = {
        5: (bollinger_5_upper, bollinger_5_lower),
        10: (bollinger_10_upper, bollinger_10_lower),
        22: (bollinger_22_upper, bollinger_22_lower),
        66: (bollinger_66_upper, bollinger_66_lower),
        132: (bollinger_132_upper, bollinger_132_lower),
        264: (bollinger_264_upper, bollinger_264_lower)
    }
    
    ##### volome ##########
    
    # EMA
    vema_dict = {
        'vema5': vema5,
        'vema10': vema10,
        'vema22': vema22,
        'vema66': vema66,
        'vema132': vema132,
        'vema264': vema264
    }
    
    # bollinger
    vbollinger_bands = {
        5: (vbollinger_5_upper, vbollinger_5_lower),
        10: (vbollinger_10_upper, vbollinger_10_lower),
        22: (vbollinger_22_upper, vbollinger_22_lower),
        66: (vbollinger_66_upper, vbollinger_66_lower),
        132: (vbollinger_132_upper, vbollinger_132_lower),
        264: (vbollinger_264_upper, vbollinger_264_lower)
    }
    
    # ###### realtime #########
    # # 實時 sma 指標

    
    ############### Features ########################

    # 建立所有要新增的欄位及其計算結果的字典
    new_columns = {}

    # 計算EMA線之間的分離度
    for shorter in ema_dict:
        for longer in ema_dict:
            if shorter != longer and int(shorter[3:]) < int(longer[3:]):  # 確保是短期EMA除以長期EMA
                col_name = f'div_{shorter}_{longer}'
                new_columns[col_name] = ema_dict[shorter] / ema_dict[longer]

    # 計算每個EMA的變化角度
    for key, ema in ema_dict.items():
        new_columns[f'entry_angle_{key}'] = np.degrees(np.arctan(ema.diff()))

    # 計算布林帶上下限與收盤價的比率
    for period, (upper, lower) in bollinger_bands.items():
        new_columns[f'div_bb{period}u'] = close / upper
        new_columns[f'div_bb{period}l'] = close / lower
        
    # 直接加入布林指標和帶寬
    new_columns.update({
        'b5': b5,
        'b10': b10,
        'b22': b22,
        'b66': b66,
        'b132': b132,
        'b264': b264,
        'bollinger5_bandwidth': bollinger_5_upper - bollinger_5_lower,
        'bollinger10_bandwidth': bollinger_10_upper - bollinger_10_lower,
        'bollinger22_bandwidth': bollinger_22_upper - bollinger_22_lower,
        'bollinger66_bandwidth': bollinger_66_upper - bollinger_66_lower,
        'bollinger132_bandwidth': bollinger_132_upper - bollinger_132_lower,
        'bollinger264_bandwidth': bollinger_264_upper - bollinger_264_lower
    })
    
    # 然後計算這些欄位的變化量
    new_columns.update({
        'bollinger5_bandwidth_change': new_columns['bollinger5_bandwidth'] / new_columns['bollinger5_bandwidth'].shift(1),
        'bollinger10_bandwidth_change': new_columns['bollinger10_bandwidth'] / new_columns['bollinger10_bandwidth'].shift(1),
        'bollinger22_bandwidth_change': new_columns['bollinger22_bandwidth'] / new_columns['bollinger22_bandwidth'].shift(1),
        'bollinger66_bandwidth_change': new_columns['bollinger66_bandwidth'] / new_columns['bollinger66_bandwidth'].shift(1),
        'bollinger132_bandwidth_change': new_columns['bollinger132_bandwidth'] / new_columns['bollinger132_bandwidth'].shift(1),
        'bollinger264_bandwidth_change': new_columns['bollinger264_bandwidth'] / new_columns['bollinger264_bandwidth'].shift(1),
    })
    
    # # # 下一開盤價與布林帶上下限的比率
    
    # RSI、KDJ、MACD、DMI等指標
    new_columns.update({
        'rsi5': rsi5,
        'rsi14': rsi14,
        'rsi28': rsi28,
        'div_rsi5_rsi14': rsi5 / rsi14,
        'div_rsi5_rsi28': rsi5 / rsi28,
        'div_rsi14_rsi28': rsi14 / rsi28,
        'angle_rsi5': np.degrees(np.arctan(rsi5.diff())),
        'angle_rsi14': np.degrees(np.arctan(rsi14.diff())),
        'angle_rsi28': np.degrees(np.arctan(rsi28.diff())),
        'K': K,
        'D': D,
        'J': J,
        'div_K_D': K / D,
        'div_K_J': K / J,
        'div_D_J': D / J,
        'angle_K': np.degrees(np.arctan(K.diff())),
        'angle_D': np.degrees(np.arctan(D.diff())),
        'angle_J': np.degrees(np.arctan(J.diff())),
        'div_ma_sig': macd/signal,
        'angle_macd': np.degrees(np.arctan(macd.diff())),
        'angle_sig': np.degrees(np.arctan(signal.diff())),
        'angle_his': np.degrees(np.arctan(histogram.diff())),
        '+DI': di_plus,
        '-DI': di_minus,
        'ADX': adx,
        'slope_+DI': np.degrees(np.arctan(di_plus.diff())),
        'slope_-DI': np.degrees(np.arctan(di_minus.diff())),
        'slope_ADX': np.degrees(np.arctan(adx.diff())),
        '+DI_div_-DI': di_plus / di_minus,
    })
    
    # OBV、PVT等指標
    new_columns.update({
        'OBV_div_ma_5': obv / obv.rolling(window=5).mean(),
        'OBV_div_ma_10': obv / obv.rolling(window=10).mean(),
        'OBV_div_ma_20': obv / obv.rolling(window=20).mean(),
        'slope_OBV': np.degrees(np.arctan(obv.diff())),
        'Modified_OBV_div_ma_5': modified_obv / modified_obv.rolling(window=5).mean(),
        'Modified_OBV_div_ma_10': modified_obv / modified_obv.rolling(window=10).mean(),
        'Modified_OBV_div_ma_20': modified_obv / modified_obv.rolling(window=20).mean(),
        'Slope_Modified_OBV': np.degrees(np.arctan(modified_obv.diff())),
        'Modified_OBV_ratio': modified_obv / modified_obv.shift(1),
        'AD_div_ma_5': ad / ad.rolling(window=5).mean(),
        'AD_div_ma_10': ad / ad.rolling(window=10).mean(),
        'AD_div_ma_20': ad / ad.rolling(window=20).mean(),
        'slope_AD': np.degrees(np.arctan(ad.diff())),
        'AD_ratio': ad / ad.shift(1),
        'PVT': pvt,
        'PVT_div_ma_5': pvt / pvt.rolling(window=5).mean(),
        'PVT_div_ma_10': pvt / pvt.rolling(window=10).mean(),
        'PVT_div_ma_20': pvt / pvt.rolling(window=20).mean(),
        'Slope_PVT': np.degrees(np.arctan(pvt.diff())),
        'Momentum_5': mom5,
        'Momentum_10': mom10,
        'Momentum_20': mom20,
        'WR': wr,
        'WR_div_30': wr / 30,
        'WR_div_70': wr / 70,
        'IMI': imi,
        'IMI_div_50': imi / 50,
        'angle_atr': np.degrees(np.arctan(atr.diff())),
        'atr_ma_22_ratio': atr / ema22,
    })
    
    ########## volume ##################
    
    # 計算vEMA線之間的分離度
    for shorter in vema_dict:
        for longer in vema_dict:
            if shorter != longer and int(shorter[4:]) < int(longer[4:]):  # 確保是短期vEMA除以長期EMA
                col_name = f'div_{shorter}_{longer}'
                new_columns[col_name] = vema_dict[shorter] / vema_dict[longer]

    # 計算每個EMA的變化角度
    for key, ema in vema_dict.items():
        new_columns[f'entry_angle_{key}'] = np.degrees(np.arctan(ema.diff()))

    # 計算布林帶上下限與收盤價的比率
    for period, (upper, lower) in vbollinger_bands.items():
        new_columns[f'div_vbb{period}u'] = capacity / upper
        new_columns[f'div_vbb{period}l'] = capacity / lower
        
    # 直接加入布林指標和帶寬
    new_columns.update({
        'vb5': vb5,
        'vb10': vb10,
        'vb22': vb22,
        'vb66': vb66,
        'vb132': vb132,
        'vb264': vb264,
        'vbollinger5_bandwidth': vbollinger_5_upper - vbollinger_5_lower,
        'vbollinger10_bandwidth': vbollinger_10_upper - vbollinger_10_lower,
        'vbollinger22_bandwidth': vbollinger_22_upper - vbollinger_22_lower,
        'vbollinger66_bandwidth': vbollinger_66_upper - vbollinger_66_lower,
        'vbollinger132_bandwidth': vbollinger_132_upper - vbollinger_132_lower,
        'vbollinger264_bandwidth': vbollinger_264_upper - vbollinger_264_lower
    })
    
    # 然後計算這些欄位的變化量
    new_columns.update({
        'vbollinger5_bandwidth_change': new_columns['vbollinger5_bandwidth'] / new_columns['vbollinger5_bandwidth'].shift(1),
        'vbollinger10_bandwidth_change': new_columns['vbollinger10_bandwidth'] / new_columns['vbollinger10_bandwidth'].shift(1),
        'vbollinger22_bandwidth_change': new_columns['vbollinger22_bandwidth'] / new_columns['vbollinger22_bandwidth'].shift(1),
        'vbollinger66_bandwidth_change': new_columns['vbollinger66_bandwidth'] / new_columns['vbollinger66_bandwidth'].shift(1),
        'vbollinger132_bandwidth_change': new_columns['vbollinger132_bandwidth'] / new_columns['vbollinger132_bandwidth'].shift(1),
        'vbollinger264_bandwidth_change': new_columns['vbollinger264_bandwidth'] / new_columns['vbollinger264_bandwidth'].shift(1),
    })
    
    ########### realtime ################
        
    ################################################# 
    
    ############## stock formatted ######################
    open_change = (df['open'] - df['close'].shift(1)) / df['close'].shift(1) *100
    high_change = (df['high'] - df['close'].shift(1)) / df['close'].shift(1) *100
    close_change = (df['close'] - df['close'].shift(1)) / df['close'].shift(1) *100
    low_change = (df['low'] - df['close'].shift(1)) / df['close'].shift(1) *100
    
    capacity_change = (df['capacity'] - df['capacity'].shift(1)) / df['capacity'].shift(1) *100
    
    # 將這些計算結果存入字典
    new_columns.update({
        'open': open_change,
        'high': high_change,
        'close': close_change,
        'low': low_change,
        'capacity': capacity_change
    })
    
    ############### chip formatted ##########################
    
    ######## k bar ####################
    new_columns.update({
        'real_ratio': real_ratio,
        'real_ratio_shift_1': real_ratio_shift_1,
        'real_ratio_shift_2': real_ratio_shift_2,
        'real_ratio_shift_3': real_ratio_shift_3,
        'sma5_real_ratio': sma5_real_ratio,
        'sma10_real_ratio': sma10_real_ratio,
        'sma20_real_ratio': sma20_real_ratio,
        'real_ratio_div_sma5': real_ratio_div_sma5,
        'real_ratio_div_sma10': real_ratio_div_sma10,
        'real_ratio_div_sma20': real_ratio_div_sma20,
        'sma5_div_sma10': sma5_div_sma10,
        'sma5_div_sma20': sma5_div_sma20,
        'sma10_div_sma20': sma10_div_sma20,
        'yesterday_close_ratio_multiply_real_ratio': yesterday_close_ratio_multiply_real_ratio,
        'sma5_ratio_multiply_real_ratio': sma5_ratio_multiply_real_ratio,
        'sma5_ratio_multiply_real_ratio_shift_1': sma5_ratio_multiply_real_ratio_shift_1,
        'sma5_ratio_multiply_real_ratio_shift_2': sma5_ratio_multiply_real_ratio_shift_2,
        'sma5_ratio_multiply_real_ratio_shift_3': sma5_ratio_multiply_real_ratio_shift_3,
        'sma10_ratio_multiply_real_ratio': sma10_ratio_multiply_real_ratio,
        'sma10_ratio_multiply_real_ratio_shift_1': sma10_ratio_multiply_real_ratio_shift_1,
        'sma10_ratio_multiply_real_ratio_shift_2': sma10_ratio_multiply_real_ratio_shift_2,
        'sma10_ratio_multiply_real_ratio_shift_3': sma10_ratio_multiply_real_ratio_shift_3,
        'sma22_ratio_multiply_real_ratio': sma22_ratio_multiply_real_ratio,
        'sma22_ratio_multiply_real_ratio_shift_1': sma22_ratio_multiply_real_ratio_shift_1,
        'sma22_ratio_multiply_real_ratio_shift_2': sma22_ratio_multiply_real_ratio_shift_2,
        'sma22_ratio_multiply_real_ratio_shift_3': sma22_ratio_multiply_real_ratio_shift_3,
        'bollinger_b5_multiply_real_ratio': bollinger_b5_multiply_real_ratio,
        'bollinger_b5_multiply_real_ratio_shift_1': bollinger_b5_multiply_real_ratio_shift_1,
        'bollinger_b5_multiply_real_ratio_shift_2': bollinger_b5_multiply_real_ratio_shift_2,
        'bollinger_b5_multiply_real_ratio_shift_3': bollinger_b5_multiply_real_ratio_shift_3,
        'bollinger_b10_multiply_real_ratio': bollinger_b10_multiply_real_ratio,
        'bollinger_b10_multiply_real_ratio_shift_1': bollinger_b10_multiply_real_ratio_shift_1,
        'bollinger_b10_multiply_real_ratio_shift_2': bollinger_b10_multiply_real_ratio_shift_2,
        'bollinger_b10_multiply_real_ratio_shift_3': bollinger_b10_multiply_real_ratio_shift_3,
        'bollinger_b22_multiply_real_ratio': bollinger_b22_multiply_real_ratio,
        'bollinger_b22_multiply_real_ratio_shift_1': bollinger_b22_multiply_real_ratio_shift_1,
        'bollinger_b22_multiply_real_ratio_shift_2': bollinger_b22_multiply_real_ratio_shift_2,
        'bollinger_b22_multiply_real_ratio_shift_3': bollinger_b22_multiply_real_ratio_shift_3,
        'real_ratio_volume_weighted': real_ratio_volume_weighted,
        'real_ratio_volume_weighted_shift_1': real_ratio_volume_weighted_shift_1,
        'real_ratio_volume_weighted_shift_2': real_ratio_volume_weighted_shift_2,
        'real_ratio_volume_weighted_shift_3': real_ratio_volume_weighted_shift_3,
        'sma5_real_ratio_volume_weighted': sma5_real_ratio_volume_weighted,
        'sma10_real_ratio_volume_weighted': sma10_real_ratio_volume_weighted,
        'sma20_real_ratio_volume_weighted': sma20_real_ratio_volume_weighted,
        'real_ratio_volume_weighted_div_sma5': real_ratio_volume_weighted_div_sma5,
        'real_ratio_volume_weighted_div_sma10': real_ratio_volume_weighted_div_sma10,
        'real_ratio_volume_weighted_div_sma20': real_ratio_volume_weighted_div_sma20,
        'sma5_div_sma10_real_ratio_volume_weighted': sma5_div_sma10_real_ratio_volume_weighted,
        'sma5_div_sma20_real_ratio_volume_weighted': sma5_div_sma20_real_ratio_volume_weighted,
        'sma10_div_sma20_real_ratio_volume_weighted': sma10_div_sma20_real_ratio_volume_weighted,
        'yesterday_close_ratio_multiply_real_ratio_volume_weighted': yesterday_close_ratio_multiply_real_ratio_volume_weighted,
        'sma5_ratio_multiply_real_ratio_volume_weighted': sma5_ratio_multiply_real_ratio_volume_weighted,
        'sma5_ratio_multiply_real_ratio_volume_weighted_shift_1': sma5_ratio_multiply_real_ratio_volume_weighted_shift_1,
        'sma5_ratio_multiply_real_ratio_volume_weighted_shift_2': sma5_ratio_multiply_real_ratio_volume_weighted_shift_2,
        'sma5_ratio_multiply_real_ratio_volume_weighted_shift_3': sma5_ratio_multiply_real_ratio_volume_weighted_shift_3,
        'sma10_ratio_multiply_real_ratio_volume_weighted': sma10_ratio_multiply_real_ratio_volume_weighted,
        'sma10_ratio_multiply_real_ratio_volume_weighted_shift_1': sma10_ratio_multiply_real_ratio_volume_weighted_shift_1,
        'sma10_ratio_multiply_real_ratio_volume_weighted_shift_2': sma10_ratio_multiply_real_ratio_volume_weighted_shift_2,
        'sma10_ratio_multiply_real_ratio_volume_weighted_shift_3': sma10_ratio_multiply_real_ratio_volume_weighted_shift_3,
        'sma22_ratio_multiply_real_ratio_volume_weighted': sma22_ratio_multiply_real_ratio_volume_weighted,
        'sma22_ratio_multiply_real_ratio_volume_weighted_shift_1': sma22_ratio_multiply_real_ratio_volume_weighted_shift_1,
        'sma22_ratio_multiply_real_ratio_volume_weighted_shift_2': sma22_ratio_multiply_real_ratio_volume_weighted_shift_2,
        'sma22_ratio_multiply_real_ratio_volume_weighted_shift_3': sma22_ratio_multiply_real_ratio_volume_weighted_shift_3,
        'bollinger_b5_multiply_real_ratio_volume_weighted': bollinger_b5_multiply_real_ratio_volume_weighted,
        'bollinger_b5_multiply_real_ratio_volume_weighted_shift_1': bollinger_b5_multiply_real_ratio_volume_weighted_shift_1,
        'bollinger_b5_multiply_real_ratio_volume_weighted_shift_2': bollinger_b5_multiply_real_ratio_volume_weighted_shift_2,
        'bollinger_b5_multiply_real_ratio_volume_weighted_shift_3': bollinger_b5_multiply_real_ratio_volume_weighted_shift_3,
        'bollinger_b10_multiply_real_ratio_volume_weighted': bollinger_b10_multiply_real_ratio_volume_weighted,
        'bollinger_b10_multiply_real_ratio_volume_weighted_shift_1': bollinger_b10_multiply_real_ratio_volume_weighted_shift_1,
        'bollinger_b10_multiply_real_ratio_volume_weighted_shift_2': bollinger_b10_multiply_real_ratio_volume_weighted_shift_2,
        'bollinger_b10_multiply_real_ratio_volume_weighted_shift_3': bollinger_b10_multiply_real_ratio_volume_weighted_shift_3,
        'bollinger_b22_multiply_real_ratio_volume_weighted': bollinger_b22_multiply_real_ratio_volume_weighted,
        'bollinger_b22_multiply_real_ratio_volume_weighted_shift_1': bollinger_b22_multiply_real_ratio_volume_weighted_shift_1,
        'bollinger_b22_multiply_real_ratio_volume_weighted_shift_2': bollinger_b22_multiply_real_ratio_volume_weighted_shift_2,
        'bollinger_b22_multiply_real_ratio_volume_weighted_shift_3': bollinger_b22_multiply_real_ratio_volume_weighted_shift_3,
        'upper_shadow_ratio': upper_shadow_ratio,
        'upper_shadow_ratio_shift_1': upper_shadow_ratio_shift_1,
        'upper_shadow_ratio_shift_2': upper_shadow_ratio_shift_2,
        'upper_shadow_ratio_shift_3': upper_shadow_ratio_shift_3,
        'yesterday_close_ratio_multiply_upper_shadow_ratio': yesterday_close_ratio_multiply_upper_shadow_ratio,
        'sma5_ratio_multiply_upper_shadow_ratio': sma5_ratio_multiply_upper_shadow_ratio,
        'sma5_ratio_multiply_upper_shadow_ratio_shift_1': sma5_ratio_multiply_upper_shadow_ratio_shift_1,
        'sma5_ratio_multiply_upper_shadow_ratio_shift_2': sma5_ratio_multiply_upper_shadow_ratio_shift_2,
        'sma5_ratio_multiply_upper_shadow_ratio_shift_3': sma5_ratio_multiply_upper_shadow_ratio_shift_3,
        'sma10_ratio_multiply_upper_shadow_ratio': sma10_ratio_multiply_upper_shadow_ratio,
        'sma10_ratio_multiply_upper_shadow_ratio_shift_1': sma10_ratio_multiply_upper_shadow_ratio_shift_1,
        'sma10_ratio_multiply_upper_shadow_ratio_shift_2': sma10_ratio_multiply_upper_shadow_ratio_shift_2,
        'sma10_ratio_multiply_upper_shadow_ratio_shift_3': sma10_ratio_multiply_upper_shadow_ratio_shift_3,
        'sma20_ratio_multiply_upper_shadow_ratio': sma20_ratio_multiply_upper_shadow_ratio,
        'sma20_ratio_multiply_upper_shadow_ratio_shift_1': sma20_ratio_multiply_upper_shadow_ratio_shift_1,
        'sma20_ratio_multiply_upper_shadow_ratio_shift_2': sma20_ratio_multiply_upper_shadow_ratio_shift_2,
        'sma20_ratio_multiply_upper_shadow_ratio_shift_3': sma20_ratio_multiply_upper_shadow_ratio_shift_3,
        'bollinger_b5_multiply_upper_shadow_ratio': bollinger_b5_multiply_upper_shadow_ratio,
        'bollinger_b5_multiply_upper_shadow_ratio_shift_1': bollinger_b5_multiply_upper_shadow_ratio_shift_1,
        'bollinger_b5_multiply_upper_shadow_ratio_shift_2': bollinger_b5_multiply_upper_shadow_ratio_shift_2,
        'bollinger_b5_multiply_upper_shadow_ratio_shift_3': bollinger_b5_multiply_upper_shadow_ratio_shift_3,
        'bollinger_b10_multiply_upper_shadow_ratio': bollinger_b10_multiply_upper_shadow_ratio,
        'bollinger_b10_multiply_upper_shadow_ratio_shift_1': bollinger_b10_multiply_upper_shadow_ratio_shift_1,
        'bollinger_b10_multiply_upper_shadow_ratio_shift_2': bollinger_b10_multiply_upper_shadow_ratio_shift_2,
        'bollinger_b10_multiply_upper_shadow_ratio_shift_3': bollinger_b10_multiply_upper_shadow_ratio_shift_3,
        'bollinger_b22_multiply_upper_shadow_ratio': bollinger_b22_multiply_upper_shadow_ratio,
        'bollinger_b22_multiply_upper_shadow_ratio_shift_1': bollinger_b22_multiply_upper_shadow_ratio_shift_1,
        'bollinger_b22_multiply_upper_shadow_ratio_shift_2': bollinger_b22_multiply_upper_shadow_ratio_shift_2,
        'bollinger_b22_multiply_upper_shadow_ratio_shift_3': bollinger_b22_multiply_upper_shadow_ratio_shift_3,
        'upper_shadow_ratio_volume_weighted': upper_shadow_ratio_volume_weighted,
        'upper_shadow_ratio_volume_weighted_shift_1': upper_shadow_ratio_volume_weighted_shift_1,
        'upper_shadow_ratio_volume_weighted_shift_2': upper_shadow_ratio_volume_weighted_shift_2,
        'upper_shadow_ratio_volume_weighted_shift_3': upper_shadow_ratio_volume_weighted_shift_3,
        'sma5_ratio_multiply_upper_shadow_volume_weighted': sma5_ratio_multiply_upper_shadow_volume_weighted,
        'sma5_ratio_multiply_upper_shadow_volume_weighted_shift_1': sma5_ratio_multiply_upper_shadow_volume_weighted_shift_1,
        'sma5_ratio_multiply_upper_shadow_volume_weighted_shift_2': sma5_ratio_multiply_upper_shadow_volume_weighted_shift_2,
        'sma5_ratio_multiply_upper_shadow_volume_weighted_shift_3': sma5_ratio_multiply_upper_shadow_volume_weighted_shift_3,
        'sma10_ratio_multiply_upper_shadow_volume_weighted': sma10_ratio_multiply_upper_shadow_volume_weighted,
        'sma10_ratio_multiply_upper_shadow_volume_weighted_shift_1': sma10_ratio_multiply_upper_shadow_volume_weighted_shift_1,
        'sma10_ratio_multiply_upper_shadow_volume_weighted_shift_2': sma10_ratio_multiply_upper_shadow_volume_weighted_shift_2,
        'sma10_ratio_multiply_upper_shadow_volume_weighted_shift_3': sma10_ratio_multiply_upper_shadow_volume_weighted_shift_3,
        'sma20_ratio_multiply_upper_shadow_volume_weighted': sma20_ratio_multiply_upper_shadow_volume_weighted,
        'sma20_ratio_multiply_upper_shadow_volume_weighted_shift_1': sma20_ratio_multiply_upper_shadow_volume_weighted_shift_1,
        'sma20_ratio_multiply_upper_shadow_volume_weighted_shift_2': sma20_ratio_multiply_upper_shadow_volume_weighted_shift_2,
        'sma20_ratio_multiply_upper_shadow_volume_weighted_shift_3': sma20_ratio_multiply_upper_shadow_volume_weighted_shift_3,
        'bollinger_b5_multiply_upper_shadow_volume_weighted': bollinger_b5_multiply_upper_shadow_volume_weighted,
        'bollinger_b5_multiply_upper_shadow_volume_weighted_shift_1': bollinger_b5_multiply_upper_shadow_volume_weighted_shift_1,
        'bollinger_b5_multiply_upper_shadow_volume_weighted_shift_2': bollinger_b5_multiply_upper_shadow_volume_weighted_shift_2,
        'bollinger_b5_multiply_upper_shadow_volume_weighted_shift_3': bollinger_b5_multiply_upper_shadow_volume_weighted_shift_3,
        'bollinger_b10_multiply_upper_shadow_volume_weighted': bollinger_b10_multiply_upper_shadow_volume_weighted,
        'bollinger_b10_multiply_upper_shadow_volume_weighted_shift_1': bollinger_b10_multiply_upper_shadow_volume_weighted_shift_1,
        'bollinger_b10_multiply_upper_shadow_volume_weighted_shift_2': bollinger_b10_multiply_upper_shadow_volume_weighted_shift_2,
        'bollinger_b10_multiply_upper_shadow_volume_weighted_shift_3': bollinger_b10_multiply_upper_shadow_volume_weighted_shift_3,
        'bollinger_b22_multiply_upper_shadow_volume_weighted': bollinger_b22_multiply_upper_shadow_volume_weighted,
        'bollinger_b22_multiply_upper_shadow_volume_weighted_shift_1': bollinger_b22_multiply_upper_shadow_volume_weighted_shift_1,
        'bollinger_b22_multiply_upper_shadow_volume_weighted_shift_2': bollinger_b22_multiply_upper_shadow_volume_weighted_shift_2,
        'bollinger_b22_multiply_upper_shadow_volume_weighted_shift_3': bollinger_b22_multiply_upper_shadow_volume_weighted_shift_3,
        'lower_shadow_ratio': lower_shadow_ratio,
        'lower_shadow_ratio_shift_1': lower_shadow_ratio_shift_1,
        'lower_shadow_ratio_shift_2': lower_shadow_ratio_shift_2,
        'lower_shadow_ratio_shift_3': lower_shadow_ratio_shift_3,
        'sma5_ratio_multiply_lower_shadow': sma5_ratio_multiply_lower_shadow,
        'sma5_ratio_multiply_lower_shadow_shift_1': sma5_ratio_multiply_lower_shadow_shift_1,
        'sma5_ratio_multiply_lower_shadow_shift_2': sma5_ratio_multiply_lower_shadow_shift_2,
        'sma5_ratio_multiply_lower_shadow_shift_3': sma5_ratio_multiply_lower_shadow_shift_3,
        'sma10_ratio_multiply_lower_shadow': sma10_ratio_multiply_lower_shadow,
        'sma10_ratio_multiply_lower_shadow_shift_1': sma10_ratio_multiply_lower_shadow_shift_1,
        'sma10_ratio_multiply_lower_shadow_shift_2': sma10_ratio_multiply_lower_shadow_shift_2,
        'sma10_ratio_multiply_lower_shadow_shift_3': sma10_ratio_multiply_lower_shadow_shift_3,
        'sma20_ratio_multiply_lower_shadow': sma20_ratio_multiply_lower_shadow,
        'sma20_ratio_multiply_lower_shadow_shift_1': sma20_ratio_multiply_lower_shadow_shift_1,
        'sma20_ratio_multiply_lower_shadow_shift_2': sma20_ratio_multiply_lower_shadow_shift_2,
        'sma20_ratio_multiply_lower_shadow_shift_3': sma20_ratio_multiply_lower_shadow_shift_3,
        'bollinger_b5_multiply_lower_shadow': bollinger_b5_multiply_lower_shadow,
        'bollinger_b5_multiply_lower_shadow_shift_1': bollinger_b5_multiply_lower_shadow_shift_1,
        'bollinger_b5_multiply_lower_shadow_shift_2': bollinger_b5_multiply_lower_shadow_shift_2,
        'bollinger_b5_multiply_lower_shadow_shift_3': bollinger_b5_multiply_lower_shadow_shift_3,
        'bollinger_b10_multiply_lower_shadow': bollinger_b10_multiply_lower_shadow,
        'bollinger_b10_multiply_lower_shadow_shift_1': bollinger_b10_multiply_lower_shadow_shift_1,
        'bollinger_b10_multiply_lower_shadow_shift_2': bollinger_b10_multiply_lower_shadow_shift_2,
        'bollinger_b10_multiply_lower_shadow_shift_3': bollinger_b10_multiply_lower_shadow_shift_3,
        'bollinger_b22_multiply_lower_shadow': bollinger_b22_multiply_lower_shadow,
        'bollinger_b22_multiply_lower_shadow_shift_1': bollinger_b22_multiply_lower_shadow_shift_1,
        'bollinger_b22_multiply_lower_shadow_shift_2': bollinger_b22_multiply_lower_shadow_shift_2,
        'bollinger_b22_multiply_lower_shadow_shift_3': bollinger_b22_multiply_lower_shadow_shift_3,
        'lower_shadow_ratio_volume_weighted': lower_shadow_ratio_volume_weighted,
        'lower_shadow_ratio_volume_weighted_shift_1': lower_shadow_ratio_volume_weighted_shift_1,
        'lower_shadow_ratio_volume_weighted_shift_2': lower_shadow_ratio_volume_weighted_shift_2,
        'lower_shadow_ratio_volume_weighted_shift_3': lower_shadow_ratio_volume_weighted_shift_3,
        'sma5_ratio_multiply_lower_shadow_volume_weighted': sma5_ratio_multiply_lower_shadow_volume_weighted,
        'sma5_ratio_multiply_lower_shadow_volume_weighted_shift_1': sma5_ratio_multiply_lower_shadow_volume_weighted_shift_1,
        'sma5_ratio_multiply_lower_shadow_volume_weighted_shift_2': sma5_ratio_multiply_lower_shadow_volume_weighted_shift_2,
        'sma5_ratio_multiply_lower_shadow_volume_weighted_shift_3': sma5_ratio_multiply_lower_shadow_volume_weighted_shift_3,
        'sma10_ratio_multiply_lower_shadow_volume_weighted': sma10_ratio_multiply_lower_shadow_volume_weighted,
        'sma10_ratio_multiply_lower_shadow_volume_weighted_shift_1': sma10_ratio_multiply_lower_shadow_volume_weighted_shift_1,
        'sma10_ratio_multiply_lower_shadow_volume_weighted_shift_2': sma10_ratio_multiply_lower_shadow_volume_weighted_shift_2,
        'sma10_ratio_multiply_lower_shadow_volume_weighted_shift_3': sma10_ratio_multiply_lower_shadow_volume_weighted_shift_3,
        'sma20_ratio_multiply_lower_shadow_volume_weighted': sma20_ratio_multiply_lower_shadow_volume_weighted,
        'sma20_ratio_multiply_lower_shadow_volume_weighted_shift_1': sma20_ratio_multiply_lower_shadow_volume_weighted_shift_1,
        'sma20_ratio_multiply_lower_shadow_volume_weighted_shift_2': sma20_ratio_multiply_lower_shadow_volume_weighted_shift_2,
        'sma20_ratio_multiply_lower_shadow_volume_weighted_shift_3': sma20_ratio_multiply_lower_shadow_volume_weighted_shift_3,
        'bollinger_b5_multiply_lower_shadow_volume_weighted': bollinger_b5_multiply_lower_shadow_volume_weighted,
        'bollinger_b5_multiply_lower_shadow_volume_weighted_shift_1': bollinger_b5_multiply_lower_shadow_volume_weighted_shift_1,
        'bollinger_b5_multiply_lower_shadow_volume_weighted_shift_2': bollinger_b5_multiply_lower_shadow_volume_weighted_shift_2,
        'bollinger_b5_multiply_lower_shadow_volume_weighted_shift_3': bollinger_b5_multiply_lower_shadow_volume_weighted_shift_3,
        'bollinger_b10_multiply_lower_shadow_volume_weighted': bollinger_b10_multiply_lower_shadow_volume_weighted,
        'bollinger_b10_multiply_lower_shadow_volume_weighted_shift_1': bollinger_b10_multiply_lower_shadow_volume_weighted_shift_1,
        'bollinger_b10_multiply_lower_shadow_volume_weighted_shift_2': bollinger_b10_multiply_lower_shadow_volume_weighted_shift_2,
        'bollinger_b10_multiply_lower_shadow_volume_weighted_shift_3': bollinger_b10_multiply_lower_shadow_volume_weighted_shift_3,
        'bollinger_b22_multiply_lower_shadow_volume_weighted': bollinger_b22_multiply_lower_shadow_volume_weighted,
        'bollinger_b22_multiply_lower_shadow_volume_weighted_shift_1': bollinger_b22_multiply_lower_shadow_volume_weighted_shift_1,
        'bollinger_b22_multiply_lower_shadow_volume_weighted_shift_2': bollinger_b22_multiply_lower_shadow_volume_weighted_shift_2,
        'bollinger_b22_multiply_lower_shadow_volume_weighted_shift_3': bollinger_b22_multiply_lower_shadow_volume_weighted_shift_3,
        'shadow_ratio': shadow_ratio,
        'shadow_ratio_shift_1': shadow_ratio_shift_1,
        'shadow_ratio_shift_2': shadow_ratio_shift_2,
        'shadow_ratio_shift_3': shadow_ratio_shift_3,
        'shadow_ratio_volume_weighted': shadow_ratio_volume_weighted,
        'shadow_ratio_volume_weighted_shift_1': shadow_ratio_volume_weighted_shift_1,
        'shadow_ratio_volume_weighted_shift_2': shadow_ratio_volume_weighted_shift_2,
        'shadow_ratio_volume_weighted_shift_3': shadow_ratio_volume_weighted_shift_3,
        'shadow_difference_ratio': shadow_difference_ratio,
        'shadow_difference_ratio_shift_1': shadow_difference_ratio_shift_1,
        'shadow_difference_ratio_shift_2': shadow_difference_ratio_shift_2,
        'shadow_difference_ratio_shift_3': shadow_difference_ratio_shift_3,
        'shadow_difference_volume_weighted': shadow_difference_volume_weighted,
        'shadow_difference_volume_weighted_shift_1': shadow_difference_volume_weighted_shift_1,
        'shadow_difference_volume_weighted_shift_2': shadow_difference_volume_weighted_shift_2,
        'shadow_difference_volume_weighted_shift_3': shadow_difference_volume_weighted_shift_3,
        'adjusted_real_shadow_ratio': adjusted_real_shadow_ratio,
        'adjusted_real_shadow_ratio_shift_1': adjusted_real_shadow_ratio_shift_1,
        'adjusted_real_shadow_ratio_shift_2': adjusted_real_shadow_ratio_shift_2,
        'adjusted_real_shadow_ratio_shift_3': adjusted_real_shadow_ratio_shift_3,
        'open_ratio': open_ratio,
        'open_ratio_shift_1': open_ratio_shift_1,
        'open_ratio_shift_2': open_ratio_shift_2,
        'open_ratio_shift_3': open_ratio_shift_3,
        'sma5_open_ratio': sma5_open_ratio,
        'sma10_open_ratio': sma10_open_ratio,
        'sma20_open_ratio': sma20_open_ratio,
        'open_ratio_div_sma5': open_ratio_div_sma5,
        'open_ratio_div_sma10': open_ratio_div_sma10,
        'open_ratio_div_sma20': open_ratio_div_sma20,
        'sma5_div_sma10_open_ratio': sma5_div_sma10_open_ratio,
        'sma5_div_sma20_open_ratio': sma5_div_sma20_open_ratio,
        'sma10_div_sma20_open_ratio': sma10_div_sma20_open_ratio,
        'close_ratio': close_ratio,
        'close_ratio_shift_1': close_ratio_shift_1,
        'close_ratio_shift_2': close_ratio_shift_2,
        'close_ratio_shift_3': close_ratio_shift_3,
        'sma5_close_ratio': sma5_close_ratio,
        'sma10_close_ratio': sma10_close_ratio,
        'sma20_close_ratio': sma20_close_ratio,
        'close_ratio_div_sma5': close_ratio_div_sma5,
        'close_ratio_div_sma10': close_ratio_div_sma10,
        'close_ratio_div_sma20': close_ratio_div_sma20,
        'sma5_div_sma10_close_ratio': sma5_div_sma10_close_ratio,
        'sma5_div_sma20_close_ratio': sma5_div_sma20_close_ratio,
        'sma10_div_sma20_close_ratio': sma10_div_sma20_close_ratio,
        'high_ratio': high_ratio,
        'high_ratio_shift_1': high_ratio_shift_1,
        'high_ratio_shift_2': high_ratio_shift_2,
        'high_ratio_shift_3': high_ratio_shift_3,
        'sma5_high_ratio': sma5_high_ratio,
        'sma10_high_ratio': sma10_high_ratio,
        'sma20_high_ratio': sma20_high_ratio,
        'high_ratio_div_sma5': high_ratio_div_sma5,
        'high_ratio_div_sma10': high_ratio_div_sma10,
        'high_ratio_div_sma20': high_ratio_div_sma20,
        'sma5_div_sma10_high_ratio': sma5_div_sma10_high_ratio,
        'sma5_div_sma20_high_ratio': sma5_div_sma20_high_ratio,
        'sma10_div_sma20_high_ratio': sma10_div_sma20_high_ratio,
        'low_ratio': low_ratio,
        'low_ratio_shift_1': low_ratio_shift_1,
        'low_ratio_shift_2': low_ratio_shift_2,
        'low_ratio_shift_3': low_ratio_shift_3,
        'sma5_low_ratio': sma5_low_ratio,
        'sma10_low_ratio': sma10_low_ratio,
        'sma20_low_ratio': sma20_low_ratio,
        'low_ratio_div_sma5': low_ratio_div_sma5,
        'low_ratio_div_sma10': low_ratio_div_sma10,
        'low_ratio_div_sma20': low_ratio_div_sma20,
        'sma5_div_sma10_low_ratio': sma5_div_sma10_low_ratio,
        'sma5_div_sma20_low_ratio': sma5_div_sma20_low_ratio,
        'sma10_div_sma20_low_ratio': sma10_div_sma20_low_ratio,
        'high_open_ratio': high_open_ratio,
        'high_open_ratio_shift_1': high_open_ratio_shift_1,
        'high_open_ratio_shift_2': high_open_ratio_shift_2,
        'high_open_ratio_shift_3': high_open_ratio_shift_3,
        'sma5_high_open_ratio': sma5_high_open_ratio,
        'sma10_high_open_ratio': sma10_high_open_ratio,
        'sma20_high_open_ratio': sma20_high_open_ratio,
        'high_open_ratio_div_sma5': high_open_ratio_div_sma5,
        'high_open_ratio_div_sma10': high_open_ratio_div_sma10,
        'high_open_ratio_div_sma20': high_open_ratio_div_sma20,
        'sma5_div_sma10_high_open_ratio': sma5_div_sma10_high_open_ratio,
        'sma5_div_sma20_high_open_ratio': sma5_div_sma20_high_open_ratio,
        'sma10_div_sma20_high_open_ratio': sma10_div_sma20_high_open_ratio,
        'low_open_ratio': low_open_ratio,
        'low_open_ratio_shift_1': low_open_ratio_shift_1,
        'low_open_ratio_shift_2': low_open_ratio_shift_2,
        'low_open_ratio_shift_3': low_open_ratio_shift_3,
        'sma5_low_open_ratio': sma5_low_open_ratio,
        'sma10_low_open_ratio': sma10_low_open_ratio,
        'sma20_low_open_ratio': sma20_low_open_ratio,
        'low_open_ratio_div_sma5': low_open_ratio_div_sma5,
        'low_open_ratio_div_sma10': low_open_ratio_div_sma10,
        'low_open_ratio_div_sma20': low_open_ratio_div_sma20,
        'sma5_div_sma10_low_open_ratio': sma5_div_sma10_low_open_ratio,
        'sma5_div_sma20_low_open_ratio': sma5_div_sma20_low_open_ratio,
        'sma10_div_sma20_low_open_ratio': sma10_div_sma20_low_open_ratio,
        'high_close_ratio': high_close_ratio,
        'high_close_ratio_shift_1': high_close_ratio_shift_1,
        'high_close_ratio_shift_2': high_close_ratio_shift_2,
        'high_close_ratio_shift_3': high_close_ratio_shift_3,
        'sma5_high_close_ratio': sma5_high_close_ratio,
        'sma10_high_close_ratio': sma10_high_close_ratio,
        'sma20_high_close_ratio': sma20_high_close_ratio,
        'high_close_ratio_div_sma5': high_close_ratio_div_sma5,
        'high_close_ratio_div_sma10': high_close_ratio_div_sma10,
        'high_close_ratio_div_sma20': high_close_ratio_div_sma20,
        'sma5_div_sma10_high_close_ratio': sma5_div_sma10_high_close_ratio,
        'sma5_div_sma20_high_close_ratio': sma5_div_sma20_high_close_ratio,
        'sma10_div_sma20_high_close_ratio': sma10_div_sma20_high_close_ratio,
        'low_close_ratio': low_close_ratio,
        'low_close_ratio_shift_1': low_close_ratio_shift_1,
        'low_close_ratio_shift_2': low_close_ratio_shift_2,
        'low_close_ratio_shift_3': low_close_ratio_shift_3,
        'sma5_low_close_ratio': sma5_low_close_ratio,
        'sma10_low_close_ratio': sma10_low_close_ratio,
        'sma20_low_close_ratio': sma20_low_close_ratio,
        'low_close_ratio_div_sma5': low_close_ratio_div_sma5,
        'low_close_ratio_div_sma10': low_close_ratio_div_sma10,
        'low_close_ratio_div_sma20': low_close_ratio_div_sma20,
        'sma5_div_sma10_low_close_ratio': sma5_div_sma10_low_close_ratio,
        'sma5_div_sma20_low_close_ratio': sma5_div_sma20_low_close_ratio,
        'sma10_div_sma20_low_close_ratio': sma10_div_sma20_low_close_ratio,
        'high_low_close_ratio': high_low_close_ratio,
        'high_low_close_ratio_shift_1': high_low_close_ratio_shift_1,
        'high_low_close_ratio_shift_2': high_low_close_ratio_shift_2,
        'high_low_close_ratio_shift_3': high_low_close_ratio_shift_3,
        'high_low_close_ratio_volume_weighted': high_low_close_ratio_volume_weighted,
        'high_low_close_ratio_volume_weighted_shift_1': high_low_close_ratio_volume_weighted_shift_1,
        'high_low_close_ratio_volume_weighted_shift_2': high_low_close_ratio_volume_weighted_shift_2,
        'high_low_close_ratio_volume_weighted_shift_3': high_low_close_ratio_volume_weighted_shift_3,
        'upper_shadow_and_real_k_strength': upper_shadow_and_real_k_strength,
        'upper_shadow_and_real_k_strength_shift_1': upper_shadow_and_real_k_strength_shift_1,
        'upper_shadow_and_real_k_strength_shift_2': upper_shadow_and_real_k_strength_shift_2,
        'upper_shadow_and_real_k_strength_shift_3': upper_shadow_and_real_k_strength_shift_3,
        'lower_shadow_and_real_k_strength': lower_shadow_and_real_k_strength,
        'lower_shadow_and_real_k_strength_shift_1': lower_shadow_and_real_k_strength_shift_1,
        'lower_shadow_and_real_k_strength_shift_2': lower_shadow_and_real_k_strength_shift_2,
        'lower_shadow_and_real_k_strength_shift_3': lower_shadow_and_real_k_strength_shift_3,
        'open_high_low_ratio': open_high_low_ratio,
        'open_high_low_ratio_shift_1': open_high_low_ratio_shift_1,
        'open_high_low_ratio_shift_2': open_high_low_ratio_shift_2,
        'open_high_low_ratio_shift_3': open_high_low_ratio_shift_3,
        'close_high_low_ratio': close_high_low_ratio,
        'close_high_low_ratio_shift_1': close_high_low_ratio_shift_1,
        'close_high_low_ratio_shift_2': close_high_low_ratio_shift_2,
        'close_high_low_ratio_shift_3': close_high_low_ratio_shift_3,
        'cumulative_ratio': cumulative_ratio,
        'cumulative_low_ratio': cumulative_low_ratio,
        'three_day_low_comparison': three_day_low_comparison,
        'five_day_low_comparison': five_day_low_comparison,
        'three_day_high_comparison': three_day_high_comparison,
        'five_day_high_comparison': five_day_high_comparison,
        'three_day_negative_real_ratio': three_day_negative_real_ratio,
        'five_day_negative_real_ratio': five_day_negative_real_ratio,
        'ten_day_negative_real_ratio': ten_day_negative_real_ratio,
        'three_day_div_ten_day_negative_real_ratio': three_day_div_ten_day_negative_real_ratio,
        'three_day_positive_real_ratio': three_day_positive_real_ratio,
        'five_day_positive_real_ratio': five_day_positive_real_ratio,
        'ten_day_positive_real_ratio': ten_day_positive_real_ratio,
        'three_day_div_ten_day_positive_real_ratio': three_day_div_ten_day_positive_real_ratio,
        'overall_k_strength': overall_k_strength,
        'overall_k_strength_shift_1': overall_k_strength_shift_1,
        'overall_k_strength_shift_2': overall_k_strength_shift_2,
        'overall_k_strength_shift_3': overall_k_strength_shift_3,
        'sma5_overall_k_strength': sma5_overall_k_strength,
        'sma10_overall_k_strength': sma10_overall_k_strength,
        'sma20_overall_k_strength': sma20_overall_k_strength,
        'overall_k_strength_div_sma5': overall_k_strength_div_sma5,
        'overall_k_strength_div_sma10': overall_k_strength_div_sma10,
        'overall_k_strength_div_sma20': overall_k_strength_div_sma20,
        'sma5_div_sma10_overall_k_strength': sma5_div_sma10_overall_k_strength,
        'sma5_div_sma20_overall_k_strength': sma5_div_sma20_overall_k_strength,
        'sma10_div_sma20_overall_k_strength': sma10_div_sma20_overall_k_strength,
        'angle_sma5_overall_k_strength': angle_sma5_overall_k_strength,
        'angle_sma10_overall_k_strength': angle_sma10_overall_k_strength,
        'angle_sma20_overall_k_strength': angle_sma20_overall_k_strength,
        'sma5_ratio_multiply_overall_k_strength': sma5_ratio_multiply_overall_k_strength,
        'sma5_ratio_multiply_overall_k_strength_shift_1': sma5_ratio_multiply_overall_k_strength_shift_1,
        'sma5_ratio_multiply_overall_k_strength_shift_2': sma5_ratio_multiply_overall_k_strength_shift_2,
        'sma5_ratio_multiply_overall_k_strength_shift_3': sma5_ratio_multiply_overall_k_strength_shift_3,
        'sma10_ratio_multiply_overall_k_strength': sma10_ratio_multiply_overall_k_strength,
        'sma10_ratio_multiply_overall_k_strength_shift_1': sma10_ratio_multiply_overall_k_strength_shift_1,
        'sma10_ratio_multiply_overall_k_strength_shift_2': sma10_ratio_multiply_overall_k_strength_shift_2,
        'sma10_ratio_multiply_overall_k_strength_shift_3': sma10_ratio_multiply_overall_k_strength_shift_3,
        'sma20_ratio_multiply_overall_k_strength': sma20_ratio_multiply_overall_k_strength,
        'sma20_ratio_multiply_overall_k_strength_shift_1': sma20_ratio_multiply_overall_k_strength_shift_1,
        'sma20_ratio_multiply_overall_k_strength_shift_2': sma20_ratio_multiply_overall_k_strength_shift_2,
        'sma20_ratio_multiply_overall_k_strength_shift_3': sma20_ratio_multiply_overall_k_strength_shift_3,
        'bollinger_b5_multiply_overall_k_strength': bollinger_b5_multiply_overall_k_strength,
        'bollinger_b5_multiply_overall_k_strength_shift_1': bollinger_b5_multiply_overall_k_strength_shift_1,
        'bollinger_b5_multiply_overall_k_strength_shift_2': bollinger_b5_multiply_overall_k_strength_shift_2,
        'bollinger_b5_multiply_overall_k_strength_shift_3': bollinger_b5_multiply_overall_k_strength_shift_3,
        'bollinger_b10_multiply_overall_k_strength': bollinger_b10_multiply_overall_k_strength,
        'bollinger_b10_multiply_overall_k_strength_shift_1': bollinger_b10_multiply_overall_k_strength_shift_1,
        'bollinger_b10_multiply_overall_k_strength_shift_2': bollinger_b10_multiply_overall_k_strength_shift_2,
        'bollinger_b10_multiply_overall_k_strength_shift_3': bollinger_b10_multiply_overall_k_strength_shift_3,
        'bollinger_b22_multiply_overall_k_strength': bollinger_b22_multiply_overall_k_strength,
        'bollinger_b22_multiply_overall_k_strength_shift_1': bollinger_b22_multiply_overall_k_strength_shift_1,
        'bollinger_b22_multiply_overall_k_strength_shift_2': bollinger_b22_multiply_overall_k_strength_shift_2,
        'bollinger_b22_multiply_overall_k_strength_shift_3': bollinger_b22_multiply_overall_k_strength_shift_3,
        'overall_k_strength_volume_weighted': overall_k_strength_volume_weighted,
        'overall_k_strength_volume_weighted_shift_1': overall_k_strength_volume_weighted_shift_1,
        'overall_k_strength_volume_weighted_shift_2': overall_k_strength_volume_weighted_shift_2,
        'overall_k_strength_volume_weighted_shift_3': overall_k_strength_volume_weighted_shift_3,
        'sma5_overall_k_strength_volume_weighted': sma5_overall_k_strength_volume_weighted,
        'sma10_overall_k_strength_volume_weighted': sma10_overall_k_strength_volume_weighted,
        'sma20_overall_k_strength_volume_weighted': sma20_overall_k_strength_volume_weighted,
        'overall_k_strength_volume_weighted_div_sma5': overall_k_strength_volume_weighted_div_sma5,
        'overall_k_strength_volume_weighted_div_sma10': overall_k_strength_volume_weighted_div_sma10,
        'overall_k_strength_volume_weighted_div_sma20': overall_k_strength_volume_weighted_div_sma20,
        'sma5_div_sma10_overall_k_strength_volume_weighted': sma5_div_sma10_overall_k_strength_volume_weighted,
        'sma5_div_sma20_overall_k_strength_volume_weighted': sma5_div_sma20_overall_k_strength_volume_weighted,
        'sma10_div_sma20_overall_k_strength_volume_weighted': sma10_div_sma20_overall_k_strength_volume_weighted,
        'angle_sma5_overall_k_strength_volume_weighted': angle_sma5_overall_k_strength_volume_weighted,
        'angle_sma10_overall_k_strength_volume_weighted': angle_sma10_overall_k_strength_volume_weighted,
        'angle_sma20_overall_k_strength_volume_weighted': angle_sma20_overall_k_strength_volume_weighted,
        'sma5_ratio_multiply_overall_k_strength_volume_weighted': sma5_ratio_multiply_overall_k_strength_volume_weighted,
        'sma5_ratio_multiply_overall_k_strength_volume_weighted_shift_1': sma5_ratio_multiply_overall_k_strength_volume_weighted_shift_1,
        'sma5_ratio_multiply_overall_k_strength_volume_weighted_shift_2': sma5_ratio_multiply_overall_k_strength_volume_weighted_shift_2,
        'sma5_ratio_multiply_overall_k_strength_volume_weighted_shift_3': sma5_ratio_multiply_overall_k_strength_volume_weighted_shift_3,
        'sma10_ratio_multiply_overall_k_strength_volume_weighted': sma10_ratio_multiply_overall_k_strength_volume_weighted,
        'sma10_ratio_multiply_overall_k_strength_volume_weighted_shift_1': sma10_ratio_multiply_overall_k_strength_volume_weighted_shift_1,
        'sma10_ratio_multiply_overall_k_strength_volume_weighted_shift_2': sma10_ratio_multiply_overall_k_strength_volume_weighted_shift_2,
        'sma10_ratio_multiply_overall_k_strength_volume_weighted_shift_3': sma10_ratio_multiply_overall_k_strength_volume_weighted_shift_3,
        'sma20_ratio_multiply_overall_k_strength_volume_weighted': sma20_ratio_multiply_overall_k_strength_volume_weighted,
        'sma20_ratio_multiply_overall_k_strength_volume_weighted_shift_1': sma20_ratio_multiply_overall_k_strength_volume_weighted_shift_1,
        'sma20_ratio_multiply_overall_k_strength_volume_weighted_shift_2': sma20_ratio_multiply_overall_k_strength_volume_weighted_shift_2,
        'sma20_ratio_multiply_overall_k_strength_volume_weighted_shift_3': sma20_ratio_multiply_overall_k_strength_volume_weighted_shift_3,
        'bollinger_b5_multiply_overall_k_strength_volume_weighted': bollinger_b5_multiply_overall_k_strength_volume_weighted,
        'bollinger_b5_multiply_overall_k_strength_volume_weighted_shift_1': bollinger_b5_multiply_overall_k_strength_volume_weighted_shift_1,
        'bollinger_b5_multiply_overall_k_strength_volume_weighted_shift_2': bollinger_b5_multiply_overall_k_strength_volume_weighted_shift_2,
        'bollinger_b5_multiply_overall_k_strength_volume_weighted_shift_3': bollinger_b5_multiply_overall_k_strength_volume_weighted_shift_3,
        'bollinger_b10_multiply_overall_k_strength_volume_weighted': bollinger_b10_multiply_overall_k_strength_volume_weighted,
        'bollinger_b10_multiply_overall_k_strength_volume_weighted_shift_1': bollinger_b10_multiply_overall_k_strength_volume_weighted_shift_1,
        'bollinger_b10_multiply_overall_k_strength_volume_weighted_shift_2': bollinger_b10_multiply_overall_k_strength_volume_weighted_shift_2,
        'bollinger_b10_multiply_overall_k_strength_volume_weighted_shift_3': bollinger_b10_multiply_overall_k_strength_volume_weighted_shift_3,
        'bollinger_b22_multiply_overall_k_strength_volume_weighted': bollinger_b22_multiply_overall_k_strength_volume_weighted,
        'bollinger_b22_multiply_overall_k_strength_volume_weighted_shift_1': bollinger_b22_multiply_overall_k_strength_volume_weighted_shift_1,
        'bollinger_b22_multiply_overall_k_strength_volume_weighted_shift_2': bollinger_b22_multiply_overall_k_strength_volume_weighted_shift_2,
        'bollinger_b22_multiply_overall_k_strength_volume_weighted_shift_3': bollinger_b22_multiply_overall_k_strength_volume_weighted_shift_3
    })

    
    #######################################################
    # 找出重複的列標籤
    duplicate_columns = ['open', 'high', 'close', 'low', 'capacity']
    # 從 df 中刪除重複的列
    df = df.drop(columns=duplicate_columns)
    
    # 將所有新列一次性加入 DataFrame
    new_columns_df = pd.DataFrame(new_columns)
    df = pd.concat([df, new_columns_df], axis=1)
    
    # 定義新的欄位順序
    new_column_order = ['stock_id'] + df.columns.difference(['stock_id']).tolist()
    
    # 重新排列 DataFrame 的欄位順序
    df = df.reindex(columns=new_column_order)
    
    cols_to_shift = df.columns[df.columns.get_loc('stock_id') + 1:]
    
    # 準備一個空的字典來存放所有移動後的列
    shifted_cols_all = {}
    
    for i in range(1, shift_days + 1):
        shifted_cols = {f'{col}_prev_{i}d': df[col].shift(i) for col in cols_to_shift}
        shifted_cols_all.update(shifted_cols)
        
    # 在迴圈完成後，將所有移動後的列一次性加入 DataFrame
    shifted_df = pd.DataFrame(shifted_cols_all)
        
    # 將這些欄位添加到原始 DataFrame 中
    df = pd.concat([df, shifted_df], axis=1)
    
    df = df.iloc[264:].reset_index(drop=True)
    # 删除原数据中的空值行
    df.dropna(inplace=True)
    
    
    return df


shift_days = 1
stockid = 2301


# 定義保存路徑
save_dir = 'Stock/CUSUM/feaData'

# 創建保存目錄（如果尚未存在）
os.makedirs(save_dir, exist_ok=True)

# 讀取原始數據
df = pd.read_csv(f'Stock/MinDataSet/{stockid}.csv')

# 合併 date 和 minute 成為 datetime
df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['minute'])
# 計算 capacity
df['capacity'] = df['volume'] * 1000

# 刪除原始的 date 和 minute 列
df.drop(['date', 'minute', 'volume'], axis=1, inplace=True)

# 設定 datetime 為索引並排序
df.set_index('datetime', inplace=True)
df = df.sort_index().reset_index()

result = analyze_stock_data(df, shift_days=4)

print(result.shape)

# 定義保存文件的路徑
save_path = os.path.join(save_dir, f'{stockid}.csv')

# 保存處理後的數據到 CSV
result.to_csv(save_path, index=False)


