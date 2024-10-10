import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from collections import defaultdict
from tqdm import tqdm
from datetime import datetime

stock_code = 6405

# 读取数据集
data = pd.read_csv(f'brokerDataSet/{stock_code}.csv')

# 指定時間範圍
start_date = datetime.strptime('2024-08-28', '%Y-%m-%d').date()
end_date = datetime.strptime('2024-09-27', '%Y-%m-%d').date()

# 计算每笔交易的净持有量（买入 - 卖出）
data['net_hold'] = data['buy'] - data['sell']

# 转换 'date' 列为 datetime 格式
data['date'] = pd.to_datetime(data['date'])

# 按券商、日期和价格排序数据，为后续处理做准备
data = data.sort_values(by=['securities_trader', 'date', 'price']).reset_index(drop=True)

# 过滤数据到指定时间範圍內
filtered_data = data[(data['date'].dt.date >= start_date) & (data['date'].dt.date <= end_date)].copy()

# 獲取所有日期並排序
dates = sorted(filtered_data['date'].dt.date.unique())
print(f"分析的日期範圍從 {start_date} 到 {end_date}，共 {len(dates)} 天。")

# 所有券商的列表
brokers = data['securities_trader'].unique()


# 定义处理单个券商的函数（与之前相同）
def compute_broker_holdings(broker_data, broker):
    holdings_history = []
    holdings = defaultdict(int)  # 初始持仓为0

    # 获取该券商的交易日期
    broker_dates = sorted(broker_data['date'].dt.date.unique())
    broker_data_grouped = broker_data.groupby(broker_data['date'].dt.date)

    for date in dates:
        if date in broker_dates:
            # 获取当天的交易数据
            daily_data = broker_data_grouped.get_group(date)
            daily_data = daily_data.sort_values(by='price')

            # 复制前一天的持仓
            holdings = holdings.copy()

            # 处理当天的每笔交易
            for idx, row in daily_data.iterrows():
                price = row['price']
                net_hold = row['net_hold']

                if net_hold > 0:
                    # 买入，首先用新买入的数量补足负持仓
                    negative_prices = sorted([p for p in holdings if holdings[p] < 0])
                    for neg_price in negative_prices:
                        negative_qty = -holdings[neg_price]
                        if net_hold >= negative_qty:
                            # 全部补足该价格的负持仓
                            net_hold -= negative_qty
                            holdings[neg_price] = 0
                        else:
                            # 部分补足负持仓
                            holdings[neg_price] += net_hold
                            net_hold = 0
                            break
                    # 如果还有剩余的买入数量，增加当前价格的持仓
                    if net_hold > 0:
                        holdings[price] += net_hold
                elif net_hold < 0:
                    # 卖出，从最低价格的持仓开始扣减
                    sell_qty = -net_hold  # 转为正值
                    prices = sorted(holdings.keys())
                    for p in prices:
                        available_qty = holdings[p]
                        if available_qty <= 0:
                            # 如果当前价格的持仓小于等于0，不再扣减
                            continue
                        if available_qty >= sell_qty:
                            holdings[p] -= sell_qty
                            sell_qty = 0
                            break
                        else:
                            holdings[p] = 0
                            sell_qty -= available_qty
                    if sell_qty > 0:
                        # 持仓不足，记录负的持仓
                        holdings[price] = holdings.get(price, 0) - sell_qty
            # 保存当天的持仓，过滤掉持仓为0的价格
            filtered_holdings = {price: qty for price, qty in holdings.items() if qty != 0}
            holdings_history.append({'date': date, 'holdings': filtered_holdings})
        else:
            # 没有交易，继承前一天的持仓
            if holdings_history:
                prev_holdings = holdings_history[-1]['holdings'].copy()
            else:
                prev_holdings = holdings.copy()
            # 过滤掉持仓为0的价格
            filtered_prev_holdings = {price: qty for price, qty in prev_holdings.items() if qty != 0}
            holdings_history.append({'date': date, 'holdings': filtered_prev_holdings})

    return broker, holdings_history

# 设置批次大小
batch_size = 100  # 根据内存情况调整

# 初始化 ParquetWriter
output_file = f'analyze_broker/analyzeData/{stock_code}.parquet'
parquet_schema = None
parquet_writer = None

# 分批处理券商
for i in tqdm(range(0, len(brokers), batch_size), desc='Processing batches'):
    batch_brokers = brokers[i:i+batch_size]
    
    records = []
    for broker in batch_brokers:
        broker_data = data[data['securities_trader'] == broker]
        if broker_data.empty:
            continue
        broker, history = compute_broker_holdings(broker_data, broker)
        for entry in history:
            date = entry['date']
            holdings = entry['holdings']
            for price, qty in holdings.items():
                # 只记录持仓不为0的条目
                if qty != 0:
                    record = {'date': date, 'securities_trader': broker, 'price': price, 'holdings': qty}
                    records.append(record)
    
    # 将结果转换为 DataFrame
    holdings_df = pd.DataFrame(records)
    
    if holdings_df.empty:
        continue  # 如果该批次没有数据，跳过
    
    # 转换为 PyArrow 表格
    table = pa.Table.from_pandas(holdings_df, preserve_index=False)
    
    # 初始化 ParquetWriter
    if parquet_writer is None:
        parquet_schema = table.schema
        parquet_writer = pq.ParquetWriter(output_file, parquet_schema)
    
    # 写入数据到 Parquet 文件
    parquet_writer.write_table(table)
    
    # 释放内存
    del records, holdings_df, table

# 关闭 ParquetWriter
if parquet_writer:
    parquet_writer.close()
    print("所有数据已保存到 Parquet 文件。")

