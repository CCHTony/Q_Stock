import pandas as pd
import twstock
from tqdm import tqdm
import os
from joblib import Parallel, delayed
import datetime

# 函数：用于获取和保存单个股票数据
def fetch_and_save(stock_id, start_year, data_folder):
    # 构建 CSV 文件路径
    file_path = os.path.join(data_folder, f'{stock_id}.csv')
    
    # 如果文件已存在，读取已存在的数据
    if os.path.exists(file_path):
        existing_data = pd.read_csv(file_path)
    else:
        existing_data = pd.DataFrame()

    if not existing_data.empty:
        # 获取最新的日期
        last_date = pd.to_datetime(existing_data['date'].iloc[-1])
        
        # 检查数据是否为最新
        today = datetime.datetime.today()
        if today.weekday() >= 5:  # 周六或周日
            last_weekday = today - datetime.timedelta(days=today.weekday() - 4)  # 上周五
        else:
            last_weekday = today

        # 如果数据已是最新，则不再获取数据
        if last_date.date() >= last_weekday.date():
            return

        start_year = last_date.year
        start_month = last_date.month
    else:
        start_month = 1

    # 建立股票对象
    stock = twstock.Stock(stock_id)

    # 尝试抓取数据
    try:
        new_data = stock.fetch_from(start_year, start_month)
    except Exception as e:
        print(f"fetching {stock_id} error: {str(e)}")
        return

    # 如果新数据为空，直接返回
    if not new_data:
        print(f"No new data fetched for {stock_id}")
        return

    # 使用 pandas 创建 DataFrame
    new_data_df = pd.DataFrame(new_data)
    
    # 如果存在数据，过滤出超出最后日期的数据
    if not existing_data.empty:
        new_data_df['date'] = pd.to_datetime(new_data_df['date'])
        new_data_df = new_data_df[new_data_df['date'] > last_date]

    # 根据情况选择保存方式
    if os.path.exists(file_path):
        new_data_df.to_csv(file_path, mode='a', header=False, index=False)
    else:
        new_data_df.to_csv(file_path, index=False)

# 主程序
def main():
    # 读取股票 ID 列表
    df = pd.read_csv('taiwan_stock_codes.csv', dtype={'StockID': str})

    # 设置开始年份
    start_year = 2008

    # 确保数据文件夹存在
    data_folder = 'stockDataSet'
    os.makedirs(data_folder, exist_ok=True)

    tasks = [(row['StockID'], start_year, data_folder) for index, row in df.iterrows()]
    Parallel(n_jobs=-1)(delayed(fetch_and_save)(*task) for task in tqdm(tasks, desc='Fetch data'))

if __name__ == "__main__":
    main()
