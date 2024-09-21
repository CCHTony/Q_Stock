import pandas as pd
import numpy as np
import os

def load_filtered_data(stockid):
    input_file = f'Stock/CUSUM/min_filtered/{stockid}.csv'
    
    if not os.path.exists(input_file):
        print(f"過濾後的資料文件 {input_file} 不存在。")
        return None, None  # 修改返回值
    
    df = pd.read_csv(input_file)
    
    # 如果在儲存時沒有保留索引，需要重新設置 datetime 索引
    if 'datetime' in df.columns:
        df['datetime'] = pd.to_datetime(df['datetime'])
    else:
        df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['minute'])
    
    # 將 'datetime' 設置為索引
    df.set_index('datetime', inplace=True)
    
    df.sort_index(inplace=True)
    
    # 提取 t_events
    t_events = df.index
    
    return t_events  # 返回 t_events

def get_close_price(stockid):
    input_file = f'Stock/MinDataSet/{stockid}.csv'  # 原始的完整數據
    
    if not os.path.exists(input_file):
        print(f"原始資料文件 {input_file} 不存在。")
        return None
    
    df = pd.read_csv(input_file)
    
    # 合併日期和時間為 datetime，並設置為索引
    if 'datetime' in df.columns:
        df['datetime'] = pd.to_datetime(df['datetime'])
    else:
        df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['minute'])
    
    df.set_index('datetime', inplace=True)
    
    df.sort_index(inplace=True)
    
    close = df['close']
    
    return close

def get_daily_vol(close, span=100):
    """
    計算每日波動率，使用指數加權移動標準差。
    
    :param close: 收盤價序列（Series）。
    :param span: 指數加權移動平均的跨度。
    :return: 波動率序列（Series）。
    """
    returns = close.pct_change().dropna()
    daily_vol = returns.ewm(span=span).std()
    return daily_vol

def get_events(close, t_events, pt_sl, target, min_ret, num_threads=1, t1=False, side=None):
    """
    為三重障礙方法設置事件。
    
    :param close: 收盤價序列（Series）。
    :param t_events: 事件時間點（DatetimeIndex）。
    :param pt_sl: [pt, sl]，止盈和止損水平的倍數。
    :param target: 目標波動率（Series）。
    :param min_ret: 最小目標收益率，過濾掉過小的目標。
    :param num_threads: 線程數，默認為1。
    :param t1: 垂直障礙（Series），持倉時間限制。
    :param side: 交易方向（Series），默認為None。
    :return: 包含事件信息的DataFrame。
    """
    # 1) 目標
    target = target.reindex(t_events)
    target = target[target > min_ret]  # 過濾
    # 2) 垂直障礙
    if t1 is False:
        t1 = pd.Series(pd.NaT, index=target.index)
    # 3) 事件DataFrame
    if side is None:
        side_ = pd.Series(1., index=target.index)
        pt_ = pd.Series(pt_sl[0], index=target.index)
        sl_ = pd.Series(pt_sl[0], index=target.index)
    else:
        side_ = side.loc[target.index]
        pt_ = pd.Series(pt_sl[0], index=target.index)
        sl_ = pd.Series(pt_sl[1], index=target.index)
    events = pd.concat({'t1': t1, 'trgt': target, 'side': side_, 'pt': pt_, 'sl': sl_}, axis=1).dropna(subset=['trgt'])
    return events

def apply_pt_sl_on_t1(close, events):
    """
    计算每个事件的止盈和止损触发时间。

    :param close: 收盘价序列（Series）。
    :param events: 事件DataFrame。
    :return: 包含每个事件止盈和止损触发时间的DataFrame。
    """
    out = events[['t1']].copy(deep=True)

    for loc, t1 in events['t1'].fillna(close.index[-1]).items():
        df0 = close[loc:t1]  # 路徑價格
        df0 = (df0 / close[loc] - 1) * events.at[loc, 'side']  # 路徑收益率
        pt = events.at[loc, 'pt'] * events.at[loc, 'trgt']
        sl = -events.at[loc, 'sl'] * events.at[loc, 'trgt']
        out.at[loc, 'sl'] = df0[df0 < sl].index.min()  # 最早的止损时间
        out.at[loc, 'pt'] = df0[df0 > pt].index.min()  # 最早的止盈时间
    return out

def get_bins(events, close):
    """
    為每個事件生成標籤。
    
    :param events: 事件DataFrame。
    :param close: 收盤價序列（Series）。
    :return: 包含標籤的DataFrame。
    """
    events_ = events.dropna(subset=['t1'])
    px = events_.index.union(events_['t1'].values).drop_duplicates()
    px = close.reindex(px, method='bfill')
    
    out = pd.DataFrame(index=events_.index)
    out['t1'] = events_['t1']
    out['ret'] = px.loc[events_['t1'].values].values / px.loc[events_.index] - 1
    if 'side' in events_:
        out['ret'] *= events_['side']  # 如果有交易方向
    out['bin'] = np.sign(out['ret'])
    return out

def save_labeled_data(stockid, labels):
    output_file = f'Stock/CUSUM/LabeledData/{stockid}.csv'
    
    # 確保輸出目錄存在
    output_dir = os.path.dirname(output_file)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    labels.to_csv(output_file)
    
    print(f"標記後的資料已儲存到 {output_file}")
    
    
def getIndMatrix(barIx, t1):
    """
    構建指示矩陣，表示每個事件在每個時間點是否活躍。

    :param barIx: 所有時間點的索引（如收盤價的索引）。
    :param t1: 每個事件的結束時間（Series），索引為事件的開始時間。
    :return: 指示矩陣（DataFrame），索引為時間點，列為事件編號。
    """
    # 初始化指示矩陣，所有值為0
    indM = pd.DataFrame(0, index=barIx, columns=range(t1.shape[0]))
    
    # 遍歷每個事件，將事件期間的值設置為1
    for i, (t0, t1) in enumerate(t1.items()):
        indM.loc[t0:t1, i] = 1.
    
    return indM
    
    
def getAvgUniqueness(indM):
    """
    計算每個事件的平均唯一性。

    :param indM: 指示矩陣（DataFrame）。
    :return: 平均唯一性（Series），索引為事件編號。
    """
    # 計算每個時間點的同時事件數量 c_t
    c = indM.sum(axis=1)
    
    # 計算每個事件在其生命週期內的唯一性 u_{t,i} = 1 / c_t
    u = indM.div(c, axis=0)
    
    # 計算每個事件的平均唯一性 \bar{u}_i
    avgU = u[u > 0].mean()
    
    return avgU


def getSampleW(close, events, numCoEvents):
    """
    計算每個事件的樣本權重，考慮收益和唯一性。

    :param close: 收盤價序列（Series），索引為 datetime。
    :param events: 包含事件信息的 DataFrame。
    :param numCoEvents: 每個時間點的同時事件數量 c_t（Series）。
    :return: 樣本權重（Series），索引為事件開始時間。
    """
    # 計算對數收益率
    ret = np.log(close).diff()
    
    # 初始化樣本權重的 Series
    wght = pd.Series(index=events.index)
    
    # 遍歷每個事件，計算其權重
    for tIn, tOut in events['t1'].items():
        # 計算事件期間的權重
        # 取事件期間的對數收益，除以同時事件數量，然後求和
        wght.loc[tIn] = (ret.loc[tIn:tOut] / numCoEvents.loc[tIn:tOut]).sum()
    
    # 返回權重的絕對值
    return wght.abs()

def getTimeDecay(tW, clfLastW=1.):
    """
    應用時間衰減因子，對樣本權重進行調整。

    :param tW: 樣本權重序列（Series），索引為事件的開始時間。
    :param clfLastW: 最老樣本的權重（默認為1，即無衰減）。
    :return: 經過時間衰減調整的樣本權重（Series）。
    """
    # 按照事件的開始時間排序，並計算累積和
    clfW = tW.sort_index().cumsum()
    
    # 根據 clfLastW 計算衰減斜率
    if clfLastW >= 0:
        slope = (1. - clfLastW) / clfW.iloc[-1]
    else:
        slope = 1. / ((clfLastW + 1) * clfW.iloc[-1])
    
    # 計算常數項
    const = 1. - slope * clfW.iloc[-1]
    
    # 計算時間衰減因子
    clfW = const + slope * clfW
    
    # 將權重小於0的設置為0
    clfW[clfW < 0] = 0
    
    return clfW


def calculate_sample_weight(close, events):
    """
    計算樣本權重，考慮收益和唯一性。

    :param close: 收盤價序列（Series）。
    :param events: 包含事件信息的 DataFrame。
    :return: 樣本權重（Series）。
    """
    # 1. 構建指示矩陣
    barIx = close.index
    indM = getIndMatrix(barIx, events['t1'])
    
    # 2. 計算每個時間點的同時事件數量 c_t
    c = indM.sum(axis=1)
    
    # 3. 計算樣本權重
    sample_weight = getSampleW(close, events, c)
    
    # 4. 標準化權重
    sample_weight *= 1 / sample_weight.sum()
    
    # 5. （可選）應用時間衰減
    # decay_factor = getTimeDecay(sample_weight)
    # sample_weight *= decay_factor
    
    return sample_weight


def triple_barrier_labeling(stockid, pt_sl=[1, 1], min_ret=0.005, span=50):
    # 1. 讀取過濾後的資料
    t_events = load_filtered_data(stockid)
    if t_events is None:
        return
    
    # 2. 獲取完整的收盤價序列
    close = get_close_price(stockid)
    if close is None:
        return
    
    # 3. 計算波動率
    daily_vol = get_daily_vol(close, span=span)
    
    # 4. 定義事件
    events = get_events(close, t_events, pt_sl, daily_vol, min_ret)
    
    # 5. 應用三重障礙方法
    out = apply_pt_sl_on_t1(close, events)
    events['t1'] = out.dropna(how='all').min(axis=1)
    events = events.dropna(subset=['t1'])
    
    # 6. 生成標籤
    labels = get_bins(events, close)
    
    # 7. 計算樣本權重
    sample_weight = calculate_sample_weight(close, events)
    sample_weight = sample_weight.reindex(labels.index)  # 確保索引對齊
    labels['sample_weight'] = sample_weight

    
    # 8. 保存標記後的資料
    save_labeled_data(stockid, labels)
    
    print("三重障礙標記法已完成。")

# 最後是 main 函數或執行部分
if __name__ == "__main__":
    stockid = '2301'
    pt_sl = [1, 1]      # 止盈和止損水平的倍數
    min_ret = 0.005     # 最小目標收益率
    span = 50           # 波動率計算的移動窗口跨度
    
    triple_barrier_labeling(stockid, pt_sl=pt_sl, min_ret=min_ret, span=span)

