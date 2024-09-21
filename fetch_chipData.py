import os
import pandas as pd
from datetime import datetime, timedelta
from FinMind.data import DataLoader
from joblib import Parallel, delayed
from tqdm import tqdm
import logging

# 設置 logging 的基本配置，將日誌級別設置為 WARNING
logging.basicConfig(level=logging.WARNING)

# 定义函数获取并保存数据
def fetch_and_save(stock_id, margin_date, institutional_date, shareholding_date, data_folder):
    # 設定 token
    token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRlIjoiMjAyNC0wOC0wMSAxODozNDowOSIsInVzZXJfaWQiOiJDQ0hUb255IiwiaXAiOiI0NS4xNDQuMjI3Ljg5In0.m9XIkb_eGRJYGDkhvGxcUi2MyvR_apgOEpnPmPQsa4o'  # 请替换为您的实际 token
    api = DataLoader()
    api.login_by_token(api_token=token)

    # 設定結束日期為今天
    today = datetime.now().strftime('%Y-%m-%d')

    # 為該股票創建專屬資料夾
    stock_folder = os.path.join(data_folder, stock_id)
    os.makedirs(stock_folder, exist_ok=True)

    # 下載並保存數據
    def download_and_save(api_func, file_path, start_date, end_date):
        if start_date < end_date:
            new_data = api_func(stock_id=stock_id, start_date=start_date, end_date=end_date)
            if not new_data.empty:
                if os.path.exists(file_path):
                    existing_data = pd.read_csv(file_path)
                    new_data = pd.concat([existing_data, new_data]).drop_duplicates().reset_index(drop=True)
                new_data.to_csv(file_path, index=False)

    # 下載個股融資融券表
    download_and_save(api.taiwan_stock_margin_purchase_short_sale, f'{stock_folder}/個股融資融劵表.csv', margin_date, today)

    # 下載個股三大法人買賣表
    download_and_save(api.taiwan_stock_institutional_investors, f'{stock_folder}/個股三大法人買賣表.csv', institutional_date, today)

    # 下載外資持股表
    download_and_save(api.taiwan_stock_shareholding, f'{stock_folder}/外資持股表.csv', shareholding_date, today)

# 查找最新的資料日期
def get_latest_date(file_path):
    if not os.path.exists(file_path):
        return None
    try:
        df = pd.read_csv(file_path)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            return df['date'].max()
        else:
            logging.warning(f"'date' column not found in {file_path}.")
            return None
    except pd.errors.EmptyDataError:
        logging.warning(f"{file_path} is empty.")
        return None

# 主程序
def main():
    # 读取股票 ID 列表
    df = pd.read_csv('Stock/taiwan_stock_codes.csv', dtype={'StockID': str})

    # 確保數據文件夾存在
    data_folder = 'Stock/chipDataSet'
    os.makedirs(data_folder, exist_ok=True)

    tasks = []
    for _, row in df.iterrows():
        stock_id = row['StockID']
        
        # 檢查各個數據文件的最新日期
        margin_file = f'{data_folder}/{stock_id}/個股融資融劵表.csv'
        institutional_file = f'{data_folder}/{stock_id}/個股三大法人買賣表.csv'
        shareholding_file = f'{data_folder}/{stock_id}/外資持股表.csv'
        
        # 查找三個檔案中的最新日期，若無資料則設為2008-01-01
        margin_date = get_latest_date(margin_file) or datetime(2008, 1, 1)
        institutional_date = get_latest_date(institutional_file) or datetime(2008, 1, 1)
        shareholding_date = get_latest_date(shareholding_file) or datetime(2008, 1, 1)

        # 從最新日期的下一天開始
        margin_date = (margin_date + timedelta(days=1)).strftime('%Y-%m-%d')
        institutional_date = (institutional_date + timedelta(days=1)).strftime('%Y-%m-%d')
        shareholding_date = (shareholding_date + timedelta(days=1)).strftime('%Y-%m-%d')

        tasks.append((stock_id, margin_date, institutional_date, shareholding_date, data_folder))

    # 並行處理所有任務
    Parallel(n_jobs=-1)(delayed(fetch_and_save)(*task) for task in tqdm(tasks, desc='Fetch data'))

if __name__ == "__main__":
    main()
