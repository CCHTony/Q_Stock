import pandas as pd
import numpy as np
import os

# 讀取台灣股票代碼和名稱
stock_codes_df = pd.read_csv('taiwan_stock_codes.csv')

# 定義要計算的波動性期間（例如最近20個交易日）
VOLATILITY_PERIOD = 22

# 初始化一個列表，用於存儲每個股票的波動性
volatility_list = []

# 遍歷每個股票代碼
for index, row in stock_codes_df.iterrows():
    stock_code = row['StockID']
    stock_name = row['Stock Name']
    stock_file = f'stockDataSet/{stock_code}.csv'
    
    # 檢查股票數據文件是否存在
    if not os.path.exists(stock_file):
        print(f"股票 {stock_code} {stock_name} 的數據文件不存在，跳過。")
        continue
    
    # 讀取股票的日K資料
    stock_data = pd.read_csv(stock_file)
    
    # 檢查數據是否足夠
    if len(stock_data) < VOLATILITY_PERIOD:
        print(f"股票 {stock_code} 的數據不足 {VOLATILITY_PERIOD} 天，跳過。")
        continue
    
    # 將 'date' 列轉換為日期格式，並按日期排序
    stock_data['date'] = pd.to_datetime(stock_data['date'])
    stock_data.sort_values('date', inplace=True)
    
    # 取最近 VOLATILITY_PERIOD 天的數據，並創建明確的副本
    recent_data = stock_data.tail(VOLATILITY_PERIOD).copy()
    
    # 計算每日的對數收益率
    recent_data['log_return'] = np.log(recent_data['close'] / recent_data['close'].shift(1))
    
    # 刪除第一行的 NaN 值
    recent_data.dropna(subset=['log_return'], inplace=True)
    
    # 計算波動性（對數收益率的年化標準差）
    # 年化標準差 = 日標準差 * sqrt(252)
    daily_std = recent_data['log_return'].std()
    annualized_volatility = daily_std * np.sqrt(252)
    
    # 將結果添加到列表
    volatility_list.append({
        'StockID': stock_code,
        'Stock Name': stock_name,
        'Volatility': annualized_volatility
    })

# 將結果轉換為 DataFrame
volatility_df = pd.DataFrame(volatility_list)

# 按照波動性從高到低排序
volatility_df.sort_values('Volatility', ascending=False, inplace=True)

# 取前10名波動性最大的股票
top_10_volatility = volatility_df.head(15)

# 顯示結果
print("近期波動性最大的前10名股票：")
print(top_10_volatility[['StockID', 'Stock Name', 'Volatility']])
