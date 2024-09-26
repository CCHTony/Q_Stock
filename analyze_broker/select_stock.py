import pandas as pd
import numpy as np
import os
from datetime import datetime

# 資料夾路徑
broker_data_folder = 'brokerDataSet'
stock_data_folder = 'stockDataSet'
test_csv_path = 'test.csv'

def load_test_stocks(test_csv_path):
    """
    讀取 test.csv 並返回一個字典，鍵為 StockID，值為 Stock Name
    """
    try:
        test_df = pd.read_csv(test_csv_path, dtype={'StockID': str, 'Stock Name': str})
        test_df['StockID'] = test_df['StockID'].astype(str)
        stock_dict = dict(zip(test_df['StockID'], test_df['Stock Name']))
        return stock_dict
    except FileNotFoundError:
        print(f"未找到 {test_csv_path} 文件。")
        return {}
    except Exception as e:
        print(f"讀取 {test_csv_path} 時發生錯誤：{e}")
        return {}

def get_current_price(stock_data_filtered):
    """
    獲取指定日期範圍內的最新收盤價作為當前價格
    """
    if stock_data_filtered.empty:
        return None
    stock_data_sorted = stock_data_filtered.sort_values('date')
    current_price = stock_data_sorted.iloc[-1]['close']
    return current_price

def get_last_n_trading_days(data, target_date, n=10):
    """
    從資料中取得指定日期之前的N個交易日
    如果資料不足N天，從最早日期開始計算
    """
    # 將 target_date 轉換為 datetime 類型以進行比較
    target_date = pd.to_datetime(target_date)

    # 將 data['date'] 轉換為 datetime64[ns] 類型
    data_sorted = data.sort_values('date')
    valid_dates = data_sorted[data_sorted['date'] < target_date]['date'].unique()

    # 如果不足N天，從最早的可用日期開始
    if len(valid_dates) < n:
        print(f"可用交易日不足 {n} 天，將返回最早日期開始的資料。")
        return valid_dates
    else:
        return valid_dates[-n:]  # 取最後N個交易日


def process_stock(stock_code, stock_name, target_date, days,
                 volume_threshold_percentile=95, 
                 near_threshold=0.01):
    """
    處理單一股票，識別是否接近關鍵點位價
    """
    # 讀取券商資料
    broker_data_path = os.path.join(broker_data_folder, f'{stock_code}.csv')
    if not os.path.exists(broker_data_path):
        print(f"未找到券商資料文件：{broker_data_path}")
        return None
    
    broker_data = pd.read_csv(broker_data_path)
    broker_data['date'] = pd.to_datetime(broker_data['date'])
    
    # 取得目標日期之前的10個交易日
    last_days = get_last_n_trading_days(broker_data, target_date, n=days)
    
    # 篩選指定日期之前的10個交易日資料
    broker_data_filtered = broker_data[broker_data['date'].isin(last_days)]
    
    if broker_data_filtered.empty:
        # 沒有符合日期範圍的資料
        print(f"股票 {stock_code} ({stock_name}) 在指定日期範圍內沒有券商交易資料。")
        return None
    
    # 計算每個價格的買入量和賣出量
    price_grouped = broker_data_filtered.groupby('price').agg({
        'buy': 'sum',
        'sell': 'sum'
    }).reset_index()
    
    # 總成交量應該是買入量或賣出量的和的一半，因為買入總量等於賣出總量
    # 總成交量可以用 (buy + sell) / 2 來計算
    price_grouped['total_volume'] = (price_grouped['buy'] + price_grouped['sell']) / 2
    
    # 計算每個價格的淨持有量
    price_grouped['net_hold'] = price_grouped['buy'] - price_grouped['sell']
    
    print(price_grouped)
    
    
    # 識別關鍵點位價（基於總成交量）
    volume_threshold = np.percentile(price_grouped['total_volume'], volume_threshold_percentile)
    key_price_levels_volume = price_grouped[price_grouped['total_volume'] >= volume_threshold]['price'].tolist()
    
    # 合併關鍵點位價，並排序
    key_price_levels = sorted(list(set(key_price_levels_volume)))
    print(key_price_levels)
    
    # 讀取股票價格資料
    stock_data_path = os.path.join(stock_data_folder, f'{stock_code}.csv')
    if not os.path.exists(stock_data_path):
        print(f"未找到股票價格資料文件：{stock_data_path}")
        return None
    
    stock_data = pd.read_csv(stock_data_path)
    stock_data['date'] = pd.to_datetime(stock_data['date'])
    
    # 篩選指定日期之前的10個交易日的股票價格資料
    stock_data_filtered = stock_data[stock_data['date'].isin(last_days)]
    
    # 獲取當前價格
    current_price = get_current_price(stock_data_filtered)
    
    if current_price is None:
        print(f"股票 {stock_code} ({stock_name}) 在指定日期範圍內沒有價格資料。")
        return None
    
    # 檢查當前價格是否接近關鍵點位價
    near_price_levels = []
    for key_price in key_price_levels:
        if key_price == 0:
            continue  # 避免除以零
        if abs(current_price - key_price) / key_price <= near_threshold:
            near_price_levels.append(key_price)
    
    if near_price_levels:
        return {
            'StockID': stock_code,
            'Stock Name': stock_name,
            'current_price': current_price,
            'key_price_levels': sorted(key_price_levels),
            'near_price_levels': sorted(near_price_levels)
        }
    else:
        return None

def main():
    # 讀取 test.csv 並獲取要檢查的股票
    stock_dict = load_test_stocks(test_csv_path)
    if not stock_dict:
        print("沒有可檢查的股票。")
        return
    
    # 使用者指定的日期
    date_str = '2024-09-26'  # 請根據需要修改
    days = 1
    
    # 轉換為日期對象
    try:
        target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError as e:
        print(f"日期格式錯誤：{e}")
        return
    
    # 可調整的閾值參數
    volume_threshold_percentile = 90     # 總成交量前5%
    near_threshold = 0.01                   # 1%
    
    # 儲存接近關鍵點位價的股票
    near_key_price_stocks = []
    
    for stock_code, stock_name in stock_dict.items():
        result = process_stock(
            stock_code=stock_code,
            stock_name=stock_name,
            target_date=target_date,
            days=days,
            volume_threshold_percentile=volume_threshold_percentile,
            near_threshold=near_threshold
        )
        if result:
            near_key_price_stocks.append(result)
    
    # 輸出結果
    if not near_key_price_stocks:
        print(f"在 {target_date} 之前10個交易日內，沒有股票接近關鍵點位價。")
    else:
        print(f"在 {target_date} 之前10個交易日內，接近關鍵點位價的股票如下：\n")
        for stock_info in near_key_price_stocks:
            print(f"股票代碼：{stock_info['StockID']} ({stock_info['Stock Name']})")
            print(f"當前價格：{stock_info['current_price']:.2f}")
            print(f"關鍵點位價：{stock_info['key_price_levels']}")
            print(f"接近的關鍵點位價：{stock_info['near_price_levels']}")
            print("----------------------------------------------------")

if __name__ == "__main__":
    main()
