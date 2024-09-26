import pandas as pd
from datetime import datetime
import numpy as np
import plotly.graph_objects as go
from plotly.colors import sample_colorscale
import plotly.express as px

# 參數設置
stock_code = 3665
# 指定時間範圍
start_date = datetime.strptime('2024-09-24', '%Y-%m-%d').date()
end_date = datetime.strptime('2024-09-25', '%Y-%m-%d').date()
# 定義要顯示的前N名
TOP_N = 1000
# 顏色映射範圍
COLOR_SCALE_MIN = 0.6
COLOR_SCALE_MAX = 0.9
# 閾值，決定是否在柱狀圖內顯示標籤
LABEL_THRESHOLD = 0.05  # 可以根據需要調整

# 步驟 1：讀取預處理好的累積持倉數據
data_path = f'analyze_broker/analyzeData/{stock_code}.parquet'
data = pd.read_parquet(data_path)

# 步驟 2：篩選指定日期範圍內的數據
data['date'] = pd.to_datetime(data['date']).dt.date
filtered_data = data[(data['date'] >= start_date) & (data['date'] <= end_date)]

# 步驟 3：獲取所有唯一的日期並排序
dates = filtered_data['date'].unique()
dates = np.sort(dates)  # 確保日期有序

# 步驟 4：獲取所有可能的買入和賣出的券商（基於持倉量）
buy_traders = filtered_data[filtered_data['holdings'] > 0]['securities_trader'].unique()
sell_traders = filtered_data[filtered_data['holdings'] < 0]['securities_trader'].unique()

num_buy_traders = len(buy_traders)
num_sell_traders = len(sell_traders)

# 使用更飽和的 'Reds' 和 'Greens' colormap 來生成顏色
buy_colorscale = px.colors.sequential.Reds
sell_colorscale = px.colors.sequential.Greens

# 避免顏色過於淺，調整色彩映射範圍
buy_colors = sample_colorscale(buy_colorscale, np.linspace(COLOR_SCALE_MIN, COLOR_SCALE_MAX, num_buy_traders))
sell_colors = sample_colorscale(sell_colorscale, np.linspace(COLOR_SCALE_MIN, COLOR_SCALE_MAX, num_sell_traders))

# 創建買入和賣出的顏色映射字典
buy_color_map = {trader: color for trader, color in zip(buy_traders, buy_colors)}
sell_color_map = {trader: color for trader, color in zip(sell_traders, sell_colors)}

# 步驟 5：逐日期生成圖表
for date in dates:
    # 選擇當天的累積數據
    daily_data = filtered_data[filtered_data['date'] == date]
    
    # 按價格和交易商分組，獲取每個交易商在該價格下的持倉量
    # 假設每個日期、交易商、價格的組合是唯一的
    # 如果有多條記錄，您可能需要調整這裡
    # 例如：daily_data = daily_data.groupby(['price', 'securities_trader']).agg({'holdings': 'last'}).reset_index()
    
    # 獲取當天的所有價格，並排序
    prices = sorted(daily_data['price'].unique())
    
    # 計算每個價格的總累積持倉量
    price_total_holdings = daily_data.groupby('price')['holdings'].sum().to_dict()
    
    # 準備買入和賣出的數據
    buy_data = {}
    sell_data = {}
    
    for price in prices:
        price_data = daily_data[daily_data['price'] == price]
        
        # 獲取前TOP_N名累積持倉量 > 0 的交易商
        top_buyers = price_data[price_data['holdings'] > 0].sort_values(by='holdings', ascending=False).head(TOP_N)
        
        # 獲取前TOP_N名累積持倉量 < 0 的交易商
        top_sellers = price_data[price_data['holdings'] < 0].sort_values(by='holdings').head(TOP_N)  # 最負值在前
        
        # 準備買入數據
        for _, row in top_buyers.iterrows():
            trader = row['securities_trader']
            holding = row['holdings']
            if trader not in buy_data:
                buy_data[trader] = {'price': [], 'holdings': []}
            buy_data[trader]['price'].append(price)
            buy_data[trader]['holdings'].append(holding)
        
        # 準備賣出數據
        for _, row in top_sellers.iterrows():
            trader = row['securities_trader']
            holding = row['holdings']
            if trader not in sell_data:
                sell_data[trader] = {'price': [], 'holdings': []}
            sell_data[trader]['price'].append(price)
            sell_data[trader]['holdings'].append(holding)
    
    # 初始化繪圖
    fig = go.Figure()
    
    # 繪製買入的堆疊橫向柱狀圖
    for trader, data_dict in buy_data.items():
        holdings = data_dict['holdings']
        prices_list = data_dict['price']
        texts = []
        for holding, price in zip(holdings, prices_list):
            total_hold = price_total_holdings.get(price, 0)
            if total_hold > 0 and (holding / total_hold) > LABEL_THRESHOLD:
                text = f"{trader}: {holding}"
            else:
                text = ''
            texts.append(text)
        fig.add_trace(go.Bar(
            x=holdings,
            y=prices_list,
            name=f'{trader} 買入',
            orientation='h',
            marker_color=buy_color_map.get(trader, '#8B0000'),  # 默認深紅色
            legendgroup='buy',
            text=texts,
            textposition='inside',
            textfont=dict(color='white', size=9),
            hovertemplate='價格: %{y}<br>持倉量: %{x}<br>交易商: ' + trader,
        ))
    
    # 繪製賣出的堆疊橫向柱狀圖（負方向）
    for trader, data_dict in sell_data.items():
        holdings = data_dict['holdings']
        prices_list = data_dict['price']
        texts = []
        for holding, price in zip(holdings, prices_list):
            total_hold = price_total_holdings.get(price, 0)
            if total_hold < 0 and (abs(holding) / abs(total_hold)) > LABEL_THRESHOLD:
                text = f"{trader}: {abs(holding)}"
            else:
                text = ''
            texts.append(text)
        fig.add_trace(go.Bar(
            x=holdings,  # holdings 已經是負值
            y=prices_list,
            name=f'{trader} 賣出',
            orientation='h',
            marker_color=sell_color_map.get(trader, '#006400'),  # 默認深綠色
            legendgroup='sell',
            text=texts,
            textposition='inside',
            textfont=dict(color='white', size=9),
            hovertemplate='價格: %{y}<br>持倉量: %{x}<br>交易商: ' + trader,
        ))
    
    # 設置圖表布局
    fig.update_layout(
        barmode='relative',
        title=dict(
            text=f'{date} 券商累積持倉量分佈',
            x=0.5,
            xanchor='center',
            font=dict(size=24, family='Hiragino Sans GB', color='black'),
        ),
        xaxis_title='累積持倉量',
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
    
    # # 等待用戶輸入以繼續
    # input(f"已顯示 {date} 的圖表。按 Enter 繼續到下一個圖表...")

print("所有圖表已顯示完畢。")
