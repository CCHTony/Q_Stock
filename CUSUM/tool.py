import pandas as pd
import numpy as np
import os

def cusum_filter(price_series, threshold):
    """
    實現CUSUM過濾器（使用對數收益率）。

    :param price_series: 價格序列（如收盤價）。
    :param threshold: 閾值，決定事件觸發的靈敏度（對數收益率的絕對值）。
    :return: 觸發事件的時間點列表。
    """
    t_events = []
    s_pos = 0
    s_neg = 0

    log_returns = np.log(price_series).diff().dropna()  # 計算對數收益率

    for i in log_returns.index:
        price_move = log_returns.loc[i]
        s_pos = max(0, s_pos + price_move)
        s_neg = min(0, s_neg + price_move)

        if s_pos > threshold:
            s_pos = 0
            t_events.append(i)
        elif s_neg < -threshold:
            s_neg = 0
            t_events.append(i)
    return pd.DatetimeIndex(t_events)

def apply_cusum_filter(stockid, threshold):
    """
    讀取指定股票的資料，應用CUSUM過濾器，並將過濾後的資料儲存到指定位置。

    :param stockid: 股票代碼（如'2301'）。
    :param threshold: 閾值，決定事件觸發的靈敏度（對數收益率的絕對值）。
    """
    # 定義文件路徑
    input_file = f'Stock/MinDataSet/{stockid}.csv'
    output_file = f'Stock/CUSUM/min_filtered/{stockid}.csv'

    # 檢查輸入文件是否存在
    if not os.path.exists(input_file):
        print(f"輸入文件 {input_file} 不存在。")
        return

    # 讀取資料
    df = pd.read_csv(input_file)

    # 合併日期和時間為datetime，並設置為索引
    df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['minute'])
    df.set_index('datetime', inplace=True)

    # 確保按時間排序
    df.sort_index(inplace=True)

    # 取得價格序列
    price_series = df['close']

    # 應用CUSUM過濾器
    t_events = cusum_filter(price_series, threshold)

    # 過濾資料
    filtered_df = df.loc[t_events]

    # 確保輸出目錄存在
    output_dir = os.path.dirname(output_file)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 儲存過濾後的資料
    filtered_df.to_csv(output_file)

    print(f"過濾後的資料已儲存到 {output_file}")

# 示例使用
stockid = '2301'
threshold = 0.01  # 對數收益率的閾值，約等於1%的價格變動
apply_cusum_filter(stockid, threshold)
