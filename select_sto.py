import pandas as pd
import os
from joblib import Parallel, delayed
from tqdm import tqdm

def process_stock(row, date_input):
    stock_id = row['StockID']
    stock_name = row['Stock Name']
    broker_file = f'brokerDataSet/{stock_id}.csv'
    local_results = []

    if not os.path.exists(broker_file):
        print(f"檔案 {broker_file} 不存在，跳過。")
        return local_results

    try:
        # 讀取券商交易資料
        df = pd.read_csv(broker_file, encoding='utf-8')
    except Exception as e:
        print(f"讀取檔案 {broker_file} 時發生錯誤: {e}")
        return local_results

    # 篩選指定日期的資料
    df_date = df[df['date'] == date_input]

    if df_date.empty:
        return local_results

    # 按券商分組，計算買入和賣出總和
    grouped = df_date.groupby('securities_trader').agg({'buy': 'sum', 'sell': 'sum'})

    # 計算買超量
    grouped['net_buy'] = grouped['buy'] - grouped['sell']

    # 篩選買超超過5,000,000的券商
    brokers_over_500 = grouped[grouped['net_buy'] > 5000000]

    for broker_name, data in brokers_over_500.iterrows():
        local_results.append({
            'StockID': stock_id,
            'StockName': stock_name,
            'Broker': broker_name,
            'NetBuy': data['net_buy']
        })

    return local_results

def main():
    # 輸入指定日期
    date_input = input("請輸入指定日期(格式YYYY-MM-DD)：")

    # 讀取股票代碼檔案
    try:
        test_df = pd.read_csv('taiwan_stock_codes.csv', encoding='utf-8')
    except Exception as e:
        print(f"讀取 'taiwan_stock_codes.csv' 時發生錯誤: {e}")
        return

    # 使用 joblib 進行平行處理
    try:
        results = Parallel(n_jobs=-1, verbose=0)(
            delayed(process_stock)(row, date_input) for index, row in tqdm(test_df.iterrows(), desc='filtering', total=len(test_df))
        )
    except Exception as e:
        print(f"平行處理時發生錯誤: {e}")
        return

    # 將多工結果展平
    result_list = [item for sublist in results for item in sublist]

    # 將結果轉換為 DataFrame
    result_df = pd.DataFrame(result_list)

    if result_df.empty:
        print("沒有符合條件的資料。")
        return

    # 根據買超量進行排序
    result_df = result_df.sort_values(by='NetBuy', ascending=False)

    # 輸出結果
    print(result_df[['StockID', 'StockName', 'Broker', 'NetBuy']])

if __name__ == "__main__":
    main()
