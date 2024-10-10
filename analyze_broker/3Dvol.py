import pandas as pd
from datetime import datetime
import numpy as np
import plotly.graph_objects as go
from plotly.colors import sample_colorscale
import plotly.express as px

# 設定股票代碼
stock_code = 3715

# 步驟 8：輸入時間範圍（這裡假設您要顯示的日期範圍）
start_date = datetime.strptime('2024-09-30', '%Y-%m-%d').date()
end_date = datetime.strptime('2024-09-30', '%Y-%m-%d').date()

# 步驟 2：讀取數據集
data = pd.read_csv(f'brokerDataSet/{stock_code}.csv')

# 步驟 4：轉換 'date' 列為 datetime 格式，便於日期範圍過濾
data['date'] = pd.to_datetime(data['date']).dt.date  # 轉換為日期格式（不含時間）

# 步驟 5：按日期、價格和券商分組，計算每個券商在每個價格和日期下的買入、賣出量
grouped = data.groupby(['date', 'price', 'securities_trader']).agg({
    'buy': 'sum',
    'sell': 'sum',
}).reset_index()

# 步驟 6：獲取所有唯一的日期，並排序
all_dates = sorted(grouped['date'].unique())

# 步驟 7：定義要顯示的前N名
TOP_N = 10

# 步驟 9：篩選日期範圍內的數據
filtered_dates = [date for date in all_dates if start_date <= date <= end_date]

# 獲取所有可能的買入和賣出的券商（將在後面動態更新）
# 這裡暫時不需要預先定義，因為每個累積範圍可能不同

# 設置閾值，決定是否在柱狀圖內顯示標籤
LABEL_THRESHOLD = 0.05  # 可以根據需要調整

# 遍歷每個選定的日期，並為每個日期生成累積3天的圖表
for current_date in filtered_dates:
    # 找到當前日期在所有日期中的索引
    current_index = all_dates.index(current_date)
    
    # 獲取當前日期及前兩個交易日的索引
    cumulative_indices = range(max(0, current_index - 2), current_index + 1)
    
    # 獲取累積的日期
    cumulative_dates = [all_dates[i] for i in cumulative_indices]
    
    # 篩選這三天的數據
    cumulative_data = grouped[grouped['date'].isin(cumulative_dates)]
    
    # 步驟 10：累積3天的買入和賣出量
    cumulative_grouped = cumulative_data.groupby(['price', 'securities_trader']).agg({
        'buy': 'sum',
        'sell': 'sum',
    }).reset_index()
    
    # 獲取所有價格，並排序
    prices = sorted(cumulative_grouped['price'].unique())
    
    # 獲取所有可能的買入和賣出的券商
    buy_traders = cumulative_grouped[cumulative_grouped['buy'] > 0]['securities_trader'].unique()
    sell_traders = cumulative_grouped[cumulative_grouped['sell'] > 0]['securities_trader'].unique()
    
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
    
    # 計算每個價格的總買入和賣出量
    price_total_buy = cumulative_grouped.groupby('price')['buy'].sum().to_dict()
    price_total_sell = cumulative_grouped.groupby('price')['sell'].sum().to_dict()
    
    # 準備買入和賣出的數據
    buy_data = {}
    sell_data = {}
    
    # 獲取前TOP_N名買入和賣出量的交易商
    for price in prices:
        price_data = cumulative_grouped[cumulative_grouped['price'] == price]
        
        # 獲取前TOP_N名買入量 > 0 的交易商
        top_buyers = price_data[price_data['buy'] > 0].sort_values(by='buy', ascending=False).head(TOP_N)
        
        # 獲取前TOP_N名賣出量 > 0 的交易商
        top_sellers = price_data[price_data['sell'] > 0].sort_values(by='sell', ascending=False).head(TOP_N)
        
        # 準備買入數據
        for _, row in top_buyers.iterrows():
            trader = row['securities_trader']
            buy_volume = row['buy']
            if trader not in buy_data:
                buy_data[trader] = {'price': [], 'buy': []}
            buy_data[trader]['price'].append(price)
            buy_data[trader]['buy'].append(buy_volume)
        
        # 準備賣出數據
        for _, row in top_sellers.iterrows():
            trader = row['securities_trader']
            sell_volume = row['sell']
            if trader not in sell_data:
                sell_data[trader] = {'price': [], 'sell': []}
            sell_data[trader]['price'].append(price)
            sell_data[trader]['sell'].append(sell_volume)
    
    # 初始化繪圖
    fig = go.Figure()
    
    # 繪製買入的堆疊橫向柱狀圖
    for trader, data_dict in buy_data.items():
        buy_volumes = data_dict['buy']
        prices_list = data_dict['price']
        texts = []
        for buy_volume, price in zip(buy_volumes, prices_list):
            total_buy_volume = price_total_buy.get(price, 0)
            if total_buy_volume > 0 and (buy_volume / total_buy_volume) > LABEL_THRESHOLD:
                text = f"{trader}: {buy_volume}"
            else:
                text = ''
            texts.append(text)
        fig.add_trace(go.Bar(
            x=buy_volumes,
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
        sell_volumes = data_dict['sell']
        prices_list = data_dict['price']
        texts = []
        for sell_volume, price in zip(sell_volumes, prices_list):
            total_sell_volume = price_total_sell.get(price, 0)
            if total_sell_volume > 0 and (sell_volume / total_sell_volume) > LABEL_THRESHOLD:
                text = f"{trader}: {sell_volume}"
            else:
                text = ''
            texts.append(text)
        # 繪製負的賣出量
        fig.add_trace(go.Bar(
            x=[-v for v in sell_volumes],
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
            text=f'{current_date} 及前兩個交易日 ({", ".join(map(str, cumulative_dates))}) 券商累積買入和賣出量分佈',
            x=0.5,
            xanchor='center',
            font=dict(size=24, family='Hiragino Sans GB', color='black'),
        ),
        xaxis_title='交易量',
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
    input(f"已顯示 {current_date} 及其前兩個交易日的圖表。按 Enter 繼續到下一個圖表...")
