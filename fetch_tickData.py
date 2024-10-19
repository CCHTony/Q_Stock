import os
import pandas as pd
from datetime import datetime, timedelta
import requests
import time

# 設定 token
token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRlIjoiMjAyNC0wOC0wNCAxMToyNTowNiIsInVzZXJfaWQiOiJDQ0hUb255IiwiaXAiOiI0NS4xNDQuMjI3LjU0In0.rVJ5f7QjDtRD5ajdeRj1gMF_3sNG5q-Se8BtAtTy2lA'

# 定義使用 requests 來獲取逐筆資料的函數
def fetch_tick_data(stock_id, start_date, end_date, token):
    url = 'https://api.finmindtrade.com/api/v4/data'
    parameter = {
        "dataset": "TaiwanStockPriceTick",
        "data_id": stock_id,
        "start_date": start_date.strftime('%Y-%m-%d'),
        "token": token,
    }
    try:
        response = requests.get(url, params=parameter)
        response.raise_for_status()  # 檢查請求是否成功
        data = response.json()
        if 'data' in data and data['data']:
            df = pd.DataFrame(data['data'])
            return df
        else:
            return pd.DataFrame()  # 返回空的 DataFrame
    except Exception as e:
        print(f"Error fetching tick data for {stock_id} from {start_date} to {end_date}: {e}")
        return pd.DataFrame()

# 定義函數來保存資料
def fetch_and_save_tick(stock_id, data_folder, api_call_count, token):
    end_date = datetime.now()

    csv_file_path = os.path.join(data_folder, f'{stock_id}.csv')

    if os.path.exists(csv_file_path):
        existing_data = pd.read_csv(csv_file_path)
        existing_data['date'] = pd.to_datetime(existing_data['date'], format='%Y-%m-%d', errors='coerce')
        last_date = existing_data['date'].max()
        start_date = last_date + timedelta(days=1)
    else:
        # 根據需求設定起始日期
        start_date = datetime.strptime('2024-06-30', '%Y-%m-%d')
        existing_data = pd.DataFrame()

    if start_date.date() > end_date.date():
        print(f"No new tick data needed for {stock_id}. Latest data is up-to-date.")
        return api_call_count

    new_data = pd.DataFrame()

    current_date = start_date
    while current_date <= end_date and api_call_count < 6000:
        print(f'\rfetching tick data for {stock_id} on {current_date.strftime("%Y-%m-%d")}', end='')
        df_tick = fetch_tick_data(stock_id, current_date, current_date, token)
        if not df_tick.empty:
            new_data = pd.concat([new_data, df_tick], ignore_index=True)
        api_call_count += 1  # 增加API呼叫次數
        current_date += timedelta(days=1)

    if not new_data.empty:
        combined_data = pd.concat([existing_data, new_data], ignore_index=True)
        combined_data['date'] = pd.to_datetime(combined_data['date']).dt.strftime('%Y-%m-%d')
        combined_data.to_csv(csv_file_path, index=False)
        print(f"\nSaved tick data for {stock_id} up to {current_date - timedelta(days=1)}.")
    else:
        print(f"\nNo new tick data for {stock_id} to save.")

    return api_call_count

# 主程序
def main():
    df = pd.read_csv('taiwan_stock_codes.csv', dtype={'StockID': str})
    data_folder = 'tickDataSet'  # 更改資料夾名稱以區分日報與逐筆資料
    os.makedirs(data_folder, exist_ok=True)
    api_call_count = 0
    for _, row in df.iterrows():
        
        if api_call_count >= 6000:
            print("\nSwitching API token due to API call limit...")
            time.sleep(2700)  # 等待一小時 (可根據實際需要取消註解)
            api_call_count = 0  # 重置API呼叫次數
        
        stock_id = row['StockID']
        print(f'\nProcessing tick data for StockID: {stock_id}')
        api_call_count = fetch_and_save_tick(stock_id, data_folder, api_call_count, token)

    print("\nAll tick data fetching completed.")

if __name__ == "__main__":
    main()
