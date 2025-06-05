import pandas as pd
import os
from tqdm import tqdm
from dateutil.parser import parse

def list_netbuy_for_broker(broker_name, start_date, end_date, broker_data_path='brokerDataSet'):
    """
    列出指定券商在指定日期範圍內每日的 net_buy。
    
    :param broker_name: 券商名稱，例如 "富邦北港"
    :param start_date: 開始日期，格式 "YYYY-MM-DD"
    :param end_date: 結束日期，格式 "YYYY-MM-DD"
    :param broker_data_path: 券商資料檔案所在目錄
    :return: DataFrame 包含日期和對應的 net_buy
    """
    # 將字串日期轉為 datetime
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    
    # 初始化一個空的 DataFrame 來存儲結果
    all_netbuy = pd.DataFrame(columns=['date', 'net_buy'])
    
    # 獲取所有 brokerDataSet 資料夾中的 CSV 檔案
    broker_files = [f for f in os.listdir(broker_data_path) if f.endswith('.csv')]
    
    for file in tqdm(broker_files, desc='Processing broker files'):
        file_path = os.path.join(broker_data_path, file)
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
        except Exception as e:
            print(f"讀取檔案 {file_path} 時發生錯誤: {e}")
            continue
        
        # 確保日期欄位為 datetime 格式
        if 'date' not in df.columns:
            print(f"檔案 {file_path} 缺少 'date' 欄位，跳過。")
            continue
        
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['date'])
        
        # 篩選指定券商和日期範圍
        df_filtered = df[
            (df['securities_trader'] == broker_name) &
            (df['date'] >= start_dt) &
            (df['date'] <= end_dt)
        ].copy()
        
        if df_filtered.empty:
            continue
        
        # 使用 .loc 進行明確的賦值操作，避免 SettingWithCopyWarning
        df_filtered.loc[:, 'net_buy'] = df_filtered['buy'] - df_filtered['sell']
        
        # 按日期彙總 net_buy
        daily_netbuy = df_filtered.groupby('date').agg({'net_buy': 'sum'}).reset_index()
        
        # 過濾掉空的或全為 NA 的 DataFrame
        if not daily_netbuy.empty and not daily_netbuy.isna().all().all():
            all_netbuy = pd.concat([all_netbuy, daily_netbuy], ignore_index=True)
    
    if all_netbuy.empty:
        print(f"在 {start_date} 至 {end_date} 期間，券商 {broker_name} 沒有交易資料。")
        return pd.DataFrame()
    
    # 按日期彙總所有股票的 net_buy
    total_netbuy = all_netbuy.groupby('date').agg({'net_buy': 'sum'}).reset_index()
    
    # 排序日期
    total_netbuy = total_netbuy.sort_values(by='date')
    
    return total_netbuy

def main():
    broker_name = "富邦北港"
    start_date = "2024-09-01"
    end_date = "2024-10-01"
    
    netbuy_df = list_netbuy_for_broker(broker_name, start_date, end_date)
    
    if netbuy_df.empty:
        print("沒有符合條件的資料。")
        return
    
    # 將結果輸出
    print(f"\n{broker_name} 在 {start_date} 至 {end_date} 每日的 net_buy：")
    print(netbuy_df.to_string(index=False))

if __name__ == "__main__":
    main()
