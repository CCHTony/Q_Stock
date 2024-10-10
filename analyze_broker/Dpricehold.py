import pandas as pd
from datetime import datetime
import numpy as np
import plotly.graph_objects as go
from plotly.colors import sample_colorscale
import plotly.express as px

stock_code = 1718
# 步驟 8：輸入時間範圍
start_date = datetime.strptime('2024-09-27', '%Y-%m-%d').date()
end_date = datetime.strptime('2024-09-27', '%Y-%m-%d').date()

# 步驟 2：讀取數據集
data = pd.read_csv(f'brokerDataSet/{stock_code}.csv')

# 步驟 3：計算每筆交易的淨持有量（買入 - 賣出）
data['net_hold'] = data['buy'] - data['sell']

# 步驟 4：轉換 'date' 列為 datetime 格式，便於日期範圍過濾
data['date'] = pd.to_datetime(data['date'])

# 步驟 5：按日期、價格和券商分組，計算每個券商在每個價格和日期下的買入、賣出和淨持有量
grouped = data.groupby(['date', 'price', 'securities_trader']).agg({
    'buy': 'sum',
    'sell': 'sum',
    'net_hold': 'sum'
}).reset_index()

# 步驟 6：獲取所有唯一的日期
dates = grouped['date'].dt.date.unique()

# 步驟 7：定義要顯示的前N名
TOP_N = 10

# 步驟 9：篩選日期範圍內的數據
filtered_dates = [date for date in dates if start_date <= date <= end_date]

# 獲取所有可能的買入和賣出的券商（基於 net_hold）
buy_traders = grouped[grouped['net_hold'] > 0]['securities_trader'].unique()
sell_traders = grouped[grouped['net_hold'] < 0]['securities_trader'].unique()

num_buy_traders = len(buy_traders)
num_sell_traders = len(sell_traders)

# 使用更飽和的 'Reds' 和 'Greens' colormap 來生成顏色
buy_colorscale = px.colors.sequential.Reds
sell_colorscale = px.colors.sequential.Greens

# 避免顏色過於淺，調整色彩映射範圍
buy_colors = sample_colorscale(buy_colorscale, np.linspace(0.6, 0.9, num_buy_traders))
sell_colors = sample_colorscale(sell_colorscale, np.linspace(0.6, 0.9, num_sell_traders))

# 創建買入和賣出的顏色映射字典
buy_color_map = {trader: color for trader, color in zip(buy_traders, buy_colors)}
sell_color_map = {trader: color for trader, color in zip(sell_traders, sell_colors)}

# 設置閾值，決定是否在柱狀圖內顯示標籤
LABEL_THRESHOLD = 0.05  # 可以根據需要調整

for date in filtered_dates:
    # 選擇當天的數據
    daily_data = grouped[grouped['date'].dt.date == date]
    
    # 獲取當天的所有價格，並排序
    prices = sorted(daily_data['price'].unique())
    
    # 計算每個價格的總 net_hold
    price_total_net_hold = daily_data.groupby('price')['net_hold'].sum().to_dict()
    
    # 準備買入和賣出的數據
    buy_data = {}
    sell_data = {}
    
    for price in prices:
        price_data = daily_data[daily_data['price'] == price]
        
        # 獲取前TOP_N名 net_hold > 0 的交易商
        top_buyers = price_data[price_data['net_hold'] > 0].sort_values(by='net_hold', ascending=False).head(TOP_N)
        
        # 獲取前TOP_N名 net_hold < 0 的交易商
        top_sellers = price_data[price_data['net_hold'] < 0].sort_values(by='net_hold').head(TOP_N)  # 最負值在前
        
        # 準備買入數據
        for _, row in top_buyers.iterrows():
            trader = row['securities_trader']
            net_hold = row['net_hold']
            if trader not in buy_data:
                buy_data[trader] = {'price': [], 'net_hold': []}
            buy_data[trader]['price'].append(price)
            buy_data[trader]['net_hold'].append(net_hold)
        
        # 準備賣出數據
        for _, row in top_sellers.iterrows():
            trader = row['securities_trader']
            net_hold = row['net_hold']
            if trader not in sell_data:
                sell_data[trader] = {'price': [], 'net_hold': []}
            sell_data[trader]['price'].append(price)
            sell_data[trader]['net_hold'].append(net_hold)
    
    # 初始化繪圖
    fig = go.Figure()
    
    # 繪製買入的堆疊橫向柱狀圖
    for trader, data_dict in buy_data.items():
        net_holds = data_dict['net_hold']
        prices_list = data_dict['price']
        texts = []
        for net_hold, price in zip(net_holds, prices_list):
            total_net_hold = price_total_net_hold.get(price, 0)
            if total_net_hold > 0 and (net_hold / total_net_hold) > LABEL_THRESHOLD:
                text = f"{trader}: {net_hold}"
            else:
                text = ''
            texts.append(text)
        fig.add_trace(go.Bar(
            x=net_holds,
            y=prices_list,
            name=f'{trader} 買入',
            orientation='h',
            marker_color=buy_color_map.get(trader, '#8B0000'),  # 默認深紅色
            legendgroup='buy',
            text=texts,
            textposition='inside',
            textfont=dict(color='white', size=9),
            hovertemplate='%{y}: %{x}',
        ))
    
    # 繪製賣出的堆疊橫向柱狀圖（負方向）
    for trader, data_dict in sell_data.items():
        net_holds = data_dict['net_hold']
        prices_list = data_dict['price']
        texts = []
        for net_hold, price in zip(net_holds, prices_list):
            total_net_hold = price_total_net_hold.get(price, 0)
            if total_net_hold < 0 and (abs(net_hold) / abs(total_net_hold)) > LABEL_THRESHOLD:
                text = f"{trader}: {abs(net_hold)}"
            else:
                text = ''
            texts.append(text)
        fig.add_trace(go.Bar(
            x=net_holds,  # net_hold 已經是負值
            y=prices_list,
            name=f'{trader} 賣出',
            orientation='h',
            marker_color=sell_color_map.get(trader, '#006400'),  # 默認深綠色
            legendgroup='sell',
            text=texts,
            textposition='inside',
            textfont=dict(color='white', size=9),
            hovertemplate='%{y}: %{x}',
        ))
    
    # 設置圖表布局
    fig.update_layout(
        barmode='relative',
        title=dict(
            text=f'{date} 券商淨持有量分佈',
            x=0.5,
            xanchor='center',
            font=dict(size=24, family='Hiragino Sans GB', color='black'),
        ),
        xaxis_title='淨持有量',
        yaxis_title='價格',
        yaxis=dict(
            categoryorder='category descending',
            autorange='reversed',
            titlefont=dict(size=16),
            tickfont=dict(size=12),
        ),
        xaxis=dict(
            titlefont=dict(size=16),
            tickfont=dict(size=12),
            gridcolor='lightgrey',
            zeroline=True,
            zerolinewidth=1,
            zerolinecolor='grey',
        ),
        legend_title_text='交易商',
        legend=dict(
            font=dict(size=12),
            orientation='v',
            x=1.02,
            y=1,
            xanchor='left',
            yanchor='top',
        ),
        hovermode='y',
        width=1200,
        height=max(600, len(prices) * 30),
        margin=dict(l=100, r=200, t=100, b=100),
        font=dict(
            family='Hiragino Sans GB',  # 確保使用支持中文的字體
            size=12,
        ),
    )
    
    # 顯示圖表
    fig.show()
    
    # 等待用戶輸入以繼續
    input(f"已顯示 {date} 的圖表。按 Enter 繼續到下一個圖表...")
