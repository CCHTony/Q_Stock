import os
import time
import pandas as pd
from datetime import datetime, timedelta
from FinMind.data import DataLoader
from tqdm import tqdm

# 設定 token
token1 = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRlIjoiMjAyNC0wOC0wNCAxMToyNTowNiIsInVzZXJfaWQiOiJDQ0hUb255IiwiaXAiOiI0NS4xNDQuMjI3LjU0In0.rVJ5f7QjDtRD5ajdeRj1gMF_3sNG5q-Se8BtAtTy2lA'  # 请替换为您的实际 token
token2 = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRlIjoiMjAyNC0wOS0xMCAxNzowMDoxMiIsInVzZXJfaWQiOiJzaGFvaHVhMDUyNiIsImlwIjoiMTIzLjI0MS40NC40OSJ9.VFuunfUKH7LhCXfu28MEJef_9uL21Tqtw-wvK338Xiw'


# 定义函数获取并保存数据
def fetch_and_save(stock_id, data_folder, api_call_count, api):
    # 設定結束日期為今天
    end_date = datetime.now()

    # 定義CSV文件的路徑
    csv_file_path = os.path.join(data_folder, f'{stock_id}.csv')

    # 檢查是否存在已有的 CSV 文件
    if os.path.exists(csv_file_path):
        # 讀取已存在的CSV文件
        existing_data = pd.read_csv(csv_file_path)
        existing_data['date'] = pd.to_datetime(existing_data['date'], format='%Y-%m-%d', errors='coerce')

        # 找到最新的日期
        last_date = existing_data['date'].max()
        # 設定開始日期為最新日期的下一天
        start_date = last_date + timedelta(days=1)
    else:
        # 如果沒有CSV文件，設定開始日期為2019-01-01
        start_date = datetime.strptime('2019-01-01', '%Y-%m-%d')
        # 初始化空的 DataFrame
        existing_data = pd.DataFrame()

    # 如果開始日期是今天，則無需下載
    if start_date.date() >= end_date.date():
        print(f"No new data needed for {stock_id}. Latest data is up-to-date.")
        return api_call_count

    # 初始化空的DataFrame來存儲新的分K資料
    new_data = pd.DataFrame()

    # 逐日下載分K資料
    current_date = start_date
    while current_date <= end_date and api_call_count < 6000:
        date_str = current_date.strftime('%Y-%m-%d')
        try:
            df_kbar = api.taiwan_stock_bar(
                stock_id=stock_id,
                date=date_str  # 取得指定日期的分K資料
            )
            api_call_count += 1  # 增加API呼叫計數
            if not df_kbar.empty:  # 如果資料不為空，則加入new_data
                new_data = pd.concat([new_data, df_kbar], ignore_index=True)
        except Exception as e:
            print(f"Error fetching K-bar data for {stock_id} on {date_str}: {e}")

        # 增加一天
        current_date += timedelta(days=1)

    # 合併已有的資料和新的資料
    if not new_data.empty:
        combined_data = pd.concat([existing_data, new_data], ignore_index=True)
        # 將 'date' 欄位格式化為 YYYY-MM-DD 格式
        combined_data['date'] = pd.to_datetime(combined_data['date']).dt.strftime('%Y-%m-%d')

        # 保存合併後的所有分K資料
        combined_data.to_csv(csv_file_path, index=False)
    else:
        print(f"No new data for {stock_id} to save.")

    return api_call_count

# 主程序
def main():
    # 读取股票 ID 列表
    df = pd.read_csv('Stock/taiwan_stock_codes.csv', dtype={'StockID': str})
    # 確保數據文件夾存在
    data_folder = 'Stock/MinDataSet'
    os.makedirs(data_folder, exist_ok=True)
    token = token2
    api = DataLoader()
    api.login_by_token(api_token=token)
    api_call_count = 0
    for _, row in df.iterrows():
        if api_call_count >= 6000:
            print("change API")
            time.sleep(1)  # 等待1小時
            api_call_count = 0  # 重置API呼叫計數
            if token == token1:
                token = token2
            else:
                token = token1
            api = DataLoader()
            api.login_by_token(api_token=token)
        stock_id = row['StockID']
        api_call_count = fetch_and_save(stock_id, data_folder, api_call_count, api)

if __name__ == "__main__":
    main()
