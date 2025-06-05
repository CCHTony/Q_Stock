import pandas as pd
import os
from joblib import Parallel, delayed
from tqdm import tqdm
from dateutil.relativedelta import relativedelta

def check_broker_condition(df, stock_df, broker_name, date_input):
    # Parse date_input
    date_input_dt = pd.to_datetime(date_input)

    # Calculate start_date (3 months before date_input)
    start_date = date_input_dt - relativedelta(months=3)

    # Filter df for dates between start_date and date_input, for the broker
    df_broker = df[
        (df['date'] >= start_date) &
        (df['date'] < date_input_dt) &
        (df['securities_trader'] == broker_name)
    ].copy()

    if df_broker.empty:
        return 0, 0  # No trials, no successes

    # Calculate net_buy for each date
    df_broker['net_buy'] = df_broker['buy'] - df_broker['sell']
    net_buy_by_date = df_broker.groupby('date').agg({'net_buy': 'sum'}).reset_index()

    # For dates when net_buy > 500,000
    dates_with_net_buy = net_buy_by_date[net_buy_by_date['net_buy'] > 500000]

    if dates_with_net_buy.empty:
        return 0, 0
    
    total_trials = len(dates_with_net_buy)
    if total_trials == 0:
        return 0, 0  # No trials, no successes

    successes = 0

    # Define the condition function (modifiable for future changes)
    def condition(next_day_stock):
        high_price = next_day_stock['high'].values[0]
        open_price = next_day_stock['open'].values[0]
        return (high_price - open_price) / open_price > 0.01

    # For these dates, get the next trading day's stock data and check the condition
    for index, row in dates_with_net_buy.iterrows():
        current_date = row['date']
        # Find the next available trading date
        future_dates = stock_df[stock_df['date'] > current_date]['date']
        if future_dates.empty:
            continue
        next_day = future_dates.min()

        # Get the stock data for next_day
        next_day_stock = stock_df[stock_df['date'] == next_day]

        if next_day_stock.empty:
            continue

        if condition(next_day_stock):
            successes += 1 # Condition met

    return total_trials, successes  # Condition not met

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

    # 讀取股票價格資料
    stock_file = f'stockDataSet/{stock_id}.csv'
    if not os.path.exists(stock_file):
        print(f"檔案 {stock_file} 不存在，跳過。")
        return local_results

    try:
        stock_df = pd.read_csv(stock_file, encoding='utf-8')
    except Exception as e:
        print(f"讀取檔案 {stock_file} 時發生錯誤: {e}")
        return local_results

    # 確保日期欄位為 datetime 格式
    df['date'] = pd.to_datetime(df['date'])
    stock_df['date'] = pd.to_datetime(stock_df['date'])

    # 篩選指定日期的資料
    df_date = df[df['date'] == pd.to_datetime(date_input)]

    if df_date.empty:
        return local_results

    # 按券商分組，計算買入和賣出總和
    grouped = df_date.groupby('securities_trader').agg({'buy': 'sum', 'sell': 'sum'})

    # 計算買超量
    grouped['net_buy'] = grouped['buy'] - grouped['sell']

    # 篩選買超超過5,000,000的券商
    brokers_over_500 = grouped[grouped['net_buy'] > 500000]

    for broker_name, data in brokers_over_500.iterrows():
        total_trials, successes = check_broker_condition(df, stock_df, broker_name, date_input)
        # if total_trials == 0:
        #     continue  # Skip brokers with no trials
        success_rate = successes / total_trials if total_trials > 0 else 0
        
        if total_trials < 5:
            local_results.append({
                'StockID': stock_id,
                'StockName': stock_name,
                'Broker': broker_name,
                'NetBuy': data['net_buy']/1000,
                'TotalTrials': total_trials,
                'Successes': successes,
                'SuccessRate': success_rate
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
    
    # 儲存結果為 CSV
    output_file = f"result/results_{date_input}.csv"
    result_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"結果已儲存為 {output_file}")

    # # 輸出結果
    # print(result_df[['StockID', 'StockName', 'Broker', 'NetBuy', 'TotalTrials', 'Successes', 'SuccessRate']])

if __name__ == "__main__":
    main()
