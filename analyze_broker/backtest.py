import pandas as pd
import os
from datetime import datetime, timedelta
from tqdm import tqdm

# 資料夾路徑
broker_data_folder = 'brokerDataSet'
stock_data_folder = 'stockDataSet'
min_data_folder = 'MinDataSet'  # 新增：分鐘級別數據資料夾
test_csv_path = 'test.csv'
output_csv_path = 'select/p_select.csv'  # 選股結果輸出路徑

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
        # print(f"可用交易日不足 {n} 天，將返回最早日期開始的資料。")
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
            if n > 1 and volumes[i] > volumes[i + 1]:
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
        # print(f"未找到券商資料文件：{broker_data_path}")
        return None

    broker_data = pd.read_csv(broker_data_path)
    broker_data['date'] = pd.to_datetime(broker_data['date'])

    # 取得目標日期之前的 N 個交易日
    last_days = get_last_n_trading_days(broker_data, target_date, n=days)

    # 篩選指定日期之前的 N 個交易日資料
    broker_data_filtered = broker_data[broker_data['date'].isin(last_days)]

    if broker_data_filtered.empty:
        # 沒有符合日期範圍的資料
        # print(f"股票 {stock_code} ({stock_name}) 在指定日期範圍內沒有券商交易資料。")
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

def generate_daily_selections(stock_dict, start_date, end_date, days):
    """
    生成每日的選股結果，返回一個字典，鍵為日期，值為該日的選股結果列表
    """
    date_range = pd.date_range(start=start_date, end=end_date, freq='B')  # 只包含工作日
    daily_selections = {}

    for current_date in tqdm(date_range, desc="Generating Daily Selections"):
        selected_stocks = []
        for stock_code, stock_name in stock_dict.items():
            result = process_stock(
                stock_code=stock_code,
                stock_name=stock_name,
                target_date=current_date,
                days=days
            )
            if result:
                result['Date'] = current_date.strftime('%Y-%m-%d')
                selected_stocks.append(result)
        if selected_stocks:
            daily_selections[current_date.strftime('%Y-%m-%d')] = selected_stocks
    return daily_selections

def backtest(daily_selections):
    """
    根據每日選股結果進行回測，模擬交易策略
    """
    trades = []  # 用於記錄所有交易

    for date_str, selections in tqdm(daily_selections.items(), desc="Backtesting"):
        for stock_info in selections:
            stock_code = stock_info['StockID']
            stock_name = stock_info['Stock Name']
            lower_price = stock_info['Bottom Price (底價)']
            higher_price = stock_info['Top Price (頂價)']
            date = stock_info['Date']

            # 讀取該股票的分鐘數據
            min_data_path = os.path.join('MinDataSet', f'{stock_code}.csv')
            if not os.path.exists(min_data_path):
                # print(f"未找到分鐘數據文件：{min_data_path}")
                continue

            min_data = pd.read_csv(min_data_path)
            # 篩選當天的數據
            min_data['date'] = pd.to_datetime(min_data['date'])
            day_data = min_data[min_data['date'] == pd.to_datetime(date)]

            if day_data.empty:
                # print(f"股票 {stock_code} 在 {date} 沒有分鐘數據。")
                continue

            # 初始化交易狀態
            position = 0  # 0: 無部位，1: 多頭，-1: 空頭
            entry_price = 0
            exit_price = 0
            entry_time = None
            exit_time = None

            # 取得當天的價格序列
            day_data = day_data.sort_values(['date', 'minute'])
            day_data.reset_index(drop=True, inplace=True)

            for idx, row in day_data.iterrows():
                current_price = row['close']
                current_time = row['minute']

                if position == 0:
                    # 無部位，判斷進場
                    if current_price <= lower_price:
                        # 做多
                        position = 1
                        entry_price = lower_price
                        entry_time = current_time
                    elif current_price >= higher_price:
                        # 做空
                        position = -1
                        entry_price = higher_price
                        entry_time = current_time
                elif position == 1:
                    # 持有多單，判斷出場
                    if current_price >= higher_price:
                        # 賣出平倉
                        exit_price = higher_price
                        exit_time = current_time
                        profit = exit_price - entry_price
                        trades.append({
                            'Date': date,
                            'StockID': stock_code,
                            'Stock Name': stock_name,
                            'Position': 'Long',
                            'Entry Time': entry_time,
                            'Entry Price': entry_price,
                            'Exit Time': exit_time,
                            'Exit Price': exit_price,
                            'Profit': profit
                        })
                        position = 0  # 重置狀態
                        break  # 完成當天交易，退出循環
                elif position == -1:
                    # 持有空單，判斷出場
                    if current_price <= lower_price:
                        # 買回平倉
                        exit_price = lower_price
                        exit_time = current_time
                        profit = entry_price - exit_price
                        trades.append({
                            'Date': date,
                            'StockID': stock_code,
                            'Stock Name': stock_name,
                            'Position': 'Short',
                            'Entry Time': entry_time,
                            'Entry Price': entry_price,
                            'Exit Time': exit_time,
                            'Exit Price': exit_price,
                            'Profit': profit
                        })
                        position = 0  # 重置狀態
                        break  # 完成當天交易，退出循環

            # 日終平倉
            if position != 0:
                # 有未平倉部位，以收盤價平倉
                closing_price = day_data.iloc[-1]['close']
                exit_price = closing_price
                exit_time = day_data.iloc[-1]['minute']
                if position == 1:
                    profit = exit_price - entry_price
                    pos = 'Long'
                else:
                    profit = entry_price - exit_price
                    pos = 'Short'
                trades.append({
                    'Date': date,
                    'StockID': stock_code,
                    'Stock Name': stock_name,
                    'Position': pos,
                    'Entry Time': entry_time,
                    'Entry Price': entry_price,
                    'Exit Time': exit_time,
                    'Exit Price': exit_price,
                    'Profit': profit
                })

    # 將交易記錄轉換為 DataFrame
    trades_df = pd.DataFrame(trades)
    return trades_df

def main():
    # 讀取 test.csv 並獲取要檢查的股票
    stock_dict = load_test_stocks(test_csv_path)
    if not stock_dict:
        print("沒有可檢查的股票。")
        return

    # 回測期間設定
    start_date = '2019-01-02'  # 根據您的數據範圍調整
    end_date = '2019-12-31'
    days = 2  # 要查看的交易日數量，可根據需要修改

    # 生成每日的選股結果
    daily_selections = generate_daily_selections(stock_dict, start_date, end_date, days)

    # 將每日選股結果保存為 CSV（可選）
    # 保存為多個 CSV 或一個 CSV，這裡我們保存為一個 CSV
    all_selections = []
    for date_str, selections in daily_selections.items():
        all_selections.extend(selections)
    if all_selections:
        df_selections = pd.DataFrame(all_selections)
        df_selections.to_csv(output_csv_path, index=False, encoding='utf-8-sig')
        print(f"選股結果已保存到 {output_csv_path}")

    # 進行回測
    trades_df = backtest(daily_selections)

    # 保存回測結果
    trades_output_path = 'backtest_results.csv'
    trades_df.to_csv(trades_output_path, index=False, encoding='utf-8-sig')
    print(f"回測結果已保存到 {trades_output_path}")

    # 總體績效
    total_profit = trades_df['Profit'].sum()
    print(f"總收益：{total_profit:.2f}")
    print(f"總交易次數：{len(trades_df)}")
    print(f"平均每筆交易收益：{(total_profit / len(trades_df)):.2f}" if len(trades_df) > 0 else "無交易")

if __name__ == "__main__":
    main()
