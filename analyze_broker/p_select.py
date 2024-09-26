import pandas as pd
import os
from datetime import datetime
from tqdm import tqdm

# 資料夾路徑
broker_data_folder = 'brokerDataSet'
stock_data_folder = 'stockDataSet'
test_csv_path = 'test.csv'
output_csv_path = 'select/p_select.csv'  # 新增：輸出 CSV 的路徑

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

def find_local_peaks(volumes):
    """
    找到總成交量的局部峰值位置
    """
    peaks = []
    n = len(volumes)
    for i in range(n):
        if i == 0:
            if volumes[i] > volumes[i + 1]:
                peaks.append(i)
        elif i == n - 1:
            if volumes[i] > volumes[i - 1]:
                peaks.append(i)
        else:
            if volumes[i] > volumes[i - 1] and volumes[i] > volumes[i + 1]:
                peaks.append(i)
    return peaks

def process_stock(stock_code, stock_name, target_date, days):
    """
    處理單一股票，找出總成交量的局部峰值價格，計算頂價和底價
    """
    # 讀取券商資料
    broker_data_path = os.path.join(broker_data_folder, f'{stock_code}.csv')
    if not os.path.exists(broker_data_path):
        print(f"未找到券商資料文件：{broker_data_path}")
        return None

    broker_data = pd.read_csv(broker_data_path)
    broker_data['date'] = pd.to_datetime(broker_data['date'])

    # 取得目標日期之前的 N 個交易日
    last_days = get_last_n_trading_days(broker_data, target_date, n=days)

    # 篩選指定日期之前的 N 個交易日資料
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

    # 總成交量計算
    price_grouped['total_volume'] = (price_grouped['buy'] + price_grouped['sell']) / 2

    # 按價格升序排列
    price_grouped_sorted = price_grouped.sort_values('price')

    # 提取價格和總成交量
    prices = price_grouped_sorted['price'].values
    volumes = price_grouped_sorted['total_volume'].values

    # 找到局部峰值的位置
    peaks_indices = find_local_peaks(volumes)

    if len(peaks_indices) < 2:
        # 如果峰值數量不足兩個，則不處理
        return None

    # 獲取峰值對應的價格和成交量
    peak_prices = prices[peaks_indices]
    peak_volumes = volumes[peaks_indices]

    # 計算峰值價格之間的差異比例
    lower_price = min(peak_prices)
    higher_price = max(peak_prices)
    price_diff_ratio = (higher_price - lower_price) / lower_price

    # 如果差異超過 1.5%，選出該股票
    if price_diff_ratio > 0.015:
        return {
            'StockID': stock_code,
            'Stock Name': stock_name,
            'Bottom Price (底價)': lower_price,
            'Top Price (頂價)': higher_price,
            'Price Difference Ratio': price_diff_ratio,
            'Peak Prices': ', '.join([f"{price:.2f}" for price in peak_prices]),  # 轉換為字符串
            'Peak Volumes': ', '.join([str(volume) for volume in peak_volumes])  # 轉換為字符串
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
    days = 2  # 要查看的交易日數量，可根據需要修改

    # 轉換為日期對象
    try:
        target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError as e:
        print(f"日期格式錯誤：{e}")
        return

    # 儲存符合條件的股票
    selected_stocks = []

    for stock_code, stock_name in tqdm(stock_dict.items()):
        result = process_stock(
            stock_code=stock_code,
            stock_name=stock_name,
            target_date=target_date,
            days=days
        )
        if result:
            selected_stocks.append(result)

    # 輸出結果
    if not selected_stocks:
        print(f"在 {target_date} 之前的 {days} 個交易日內，沒有符合條件的股票。")
    else:
        print(f"在 {target_date} 之前的 {days} 個交易日內，符合條件的股票如下：\n")
        for stock_info in selected_stocks:
            print(f"股票代碼：{stock_info['StockID']} ({stock_info['Stock Name']})")
            print(f"底價：{stock_info['Bottom Price (底價)']:.2f}")
            print(f"頂價：{stock_info['Top Price (頂價)']:.2f}")
            print(f"價格差異比例：{stock_info['Price Difference Ratio']:.4f}")
            print(f"峰值價格：{stock_info['Peak Prices']}")
            print(f"峰值成交量：{stock_info['Peak Volumes']}")
            print("----------------------------------------------------")
            
        # 將結果儲存為 DataFrame
        df_selected = pd.DataFrame(selected_stocks)

        # 儲存為 CSV 文件
        try:
            df_selected.to_csv(output_csv_path, index=False, encoding='utf-8-sig')  # 使用 utf-8-sig 以支援中文
            print(f"\n結果已成功儲存到 {output_csv_path}")
        except Exception as e:
            print(f"儲存 CSV 時發生錯誤：{e}")

    print(f'共{len(selected_stocks)}檔股票')

if __name__ == "__main__":
    main()
