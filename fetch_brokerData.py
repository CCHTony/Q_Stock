import os
import time
import pandas as pd
from datetime import datetime, timedelta
from FinMind.data import DataLoader
from tqdm import tqdm
import requests

# 設定 token
token1 = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRlIjoiMjAyNC0wOC0wNCAxMToyNTowNiIsInVzZXJfaWQiOiJDQ0hUb255IiwiaXAiOiI0NS4xNDQuMjI3LjU0In0.rVJ5f7QjDtRD5ajdeRj1gMF_3sNG5q-Se8BtAtTy2lA'
token2 = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRlIjoiMjAyNC0wOS0xMCAxNzowMDoxMiIsInVzZXJfaWQiOiJzaGFvaHVhMDUyNiIsImlwIjoiMTIzLjI0MS40NC40OSJ9.VFuunfUKH7LhCXfu28MEJef_9uL21Tqtw-wvK338Xiw'

# 定義使用 requests 來獲取日報資料的函數
def fetch_daily_report(stock_id, date, token):
    url = 'https://api.finmindtrade.com/api/v4/taiwan_stock_trading_daily_report'
    parameter = {
        "data_id": stock_id,
        "date": date.strftime('%Y-%m-%d'),
        "token": token,  # 使用token進行請求
    }
    try:
        response = requests.get(url, params=parameter)
        data = response.json()
        if 'data' in data:
            df = pd.DataFrame(data['data'])
            return df
        else:
            print(f"No data returned for {stock_id} on {date}")
            return pd.DataFrame()  # 返回空的 DataFrame
    except Exception as e:
        print(f"Error fetching daily report for {stock_id} on {date}: {e}")
        return pd.DataFrame()

# 定義函數來保存資料
def fetch_and_save(stock_id, data_folder, api_call_count, api, token):
    end_date = datetime.now()

    csv_file_path = os.path.join(data_folder, f'{stock_id}.csv')

    if os.path.exists(csv_file_path):
        existing_data = pd.read_csv(csv_file_path)
        existing_data['date'] = pd.to_datetime(existing_data['date'], format='%Y-%m-%d', errors='coerce')
        last_date = existing_data['date'].max()
        start_date = last_date + timedelta(days=1)
    else:
        start_date = datetime.strptime('2019-01-01', '%Y-%m-%d')
        existing_data = pd.DataFrame()

    if start_date.date() >= end_date.date():
        print(f"No new data needed for {stock_id}. Latest data is up-to-date.")
        return api_call_count

    new_data = pd.DataFrame()

    current_date = start_date
    while current_date <= end_date and api_call_count < 6000:
        # 嘗試使用 requests 獲取每日交易報告
        df_daily_report = fetch_daily_report(stock_id, current_date, token)
        if not df_daily_report.empty:
            new_data = pd.concat([new_data, df_daily_report], ignore_index=True)
        api_call_count += 1  # 增加API呼叫次數
        current_date += timedelta(days=1)

    if not new_data.empty:
        combined_data = pd.concat([existing_data, new_data], ignore_index=True)
        combined_data['date'] = pd.to_datetime(combined_data['date']).dt.strftime('%Y-%m-%d')
        combined_data.to_csv(csv_file_path, index=False)
    else:
        print(f"No new data for {stock_id} to save.")

    return api_call_count

# 主程序
def main():
    df = pd.read_csv('Stock/taiwan_stock_codes.csv', dtype={'StockID': str})
    data_folder = 'Stock/MinDataSet'
    os.makedirs(data_folder, exist_ok=True)
    token = token2
    api = DataLoader()
    api.login_by_token(api_token=token)
    api_call_count = 0
    for _, row in tqdm(df.iterrows()):
        if api_call_count >= 6000:
            print("Switching API token...")
            time.sleep(3600)  # 等待一小時
            api_call_count = 0  # 重置API呼叫次數
            token = token1 if token == token2 else token2
        stock_id = row['StockID']
        api_call_count = fetch_and_save(stock_id, data_folder, api_call_count, api, token)

if __name__ == "__main__":
    main()
