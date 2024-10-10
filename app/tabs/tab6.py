# tabs/tab6.py

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
from plotly.colors import sample_colorscale

def render_tab6(df_period, N):
    st.subheader('交易量分佈')
    
    # 定義 min_date 和 max_date
    min_date = df_period['date'].min()
    max_date = df_period['date'].max()

    # 初始化 session_state 中的 'cumulative_entries' 如果尚未存在
    if 'cumulative_entries' not in st.session_state:
        st.session_state.cumulative_entries = [{'date': None, 'cumulative_days': 3}]

    # 定義一個函數來添加新條目
    def add_entry():
        st.session_state.cumulative_entries.append({'date': None, 'cumulative_days': 3})

    # 定義一個函數來移除條目
    def remove_entry(index):
        if len(st.session_state.cumulative_entries) > 1:
            st.session_state.cumulative_entries.pop(index)

    # 添加按鈕來添加新條目，並指定唯一的 key
    st.button('添加新日期-回朔天數組合', on_click=add_entry, key='add_entry_tab6')

    # 迭代並顯示所有條目
    for idx, entry in enumerate(st.session_state.cumulative_entries):
        with st.container():
            cols = st.columns([1, 1, 0.2])
            with cols[0]:
                # 日期選擇
                date_key = f'date_tab6_{idx}'
                current_date = st.session_state.cumulative_entries[idx]['date']
                # 驗證日期是否在範圍內
                if current_date is not None and not (min_date <= current_date <= max_date):
                    st.session_state.cumulative_entries[idx]['date'] = min_date
                    current_date = min_date
                selected_date = st.date_input(
                    f'選擇日期 #{idx + 1}',
                    value=current_date or min_date,
                    min_value=min_date,
                    max_value=max_date,
                    key=date_key
                )
            with cols[1]:
                # 回朔天數
                days_key = f'days_tab6_{idx}'
                cumulative_days = st.number_input(
                    f'回朔天數 #{idx + 1}',
                    min_value=1,
                    max_value=30,
                    value=st.session_state.cumulative_entries[idx]['cumulative_days'],
                    step=1,
                    key=days_key
                )
            with cols[2]:
                # 移除條目按鈕，並指定唯一的 key
                if idx != 0:
                    st.button('❌', key=f'remove_tab6_{idx}', on_click=remove_entry, args=(idx,))

            # 更新 session_state
            st.session_state.cumulative_entries[idx]['date'] = selected_date
            st.session_state.cumulative_entries[idx]['cumulative_days'] = cumulative_days

    # 選擇交易日期和回朔天數後生成圖表
    st.markdown('---')
    st.write('### 交易量分佈圖')

    for idx, entry in enumerate(st.session_state.cumulative_entries):
        selected_date = entry['date']
        cumulative_days = entry['cumulative_days']

        if selected_date is None:
            st.warning(f'請選擇日期 #{idx + 1}。')
            continue

        # 將選定日期轉換為 datetime.date 格式
        current_date = selected_date

        # 找到當前日期在所有日期中的索引
        all_dates = sorted(df_period['date'].unique())
        try:
            current_index = all_dates.index(current_date)
        except ValueError:
            st.warning(f'選定的日期 {current_date} 不存在於資料中。')
            continue

        # 獲取當前日期及前累計天數 - 1 個交易日的索引
        cumulative_indices = range(max(0, current_index - (cumulative_days - 1)), current_index + 1)

        # 獲取累積的日期
        cumulative_dates = [all_dates[i] for i in cumulative_indices]

        # 篩選這些天的數據
        cumulative_data = df_period[df_period['date'].isin(cumulative_dates)]

        # 按日期、價格和券商分組，計算每個券商在每個價格和日期下的買入、賣出量
        grouped_data = cumulative_data.groupby(['price', 'securities_trader']).agg({
            'buy': 'sum',
            'sell': 'sum',
        }).reset_index()

        if grouped_data.empty:
            st.warning(f'在日期 {current_date} 及其前 {cumulative_days -1} 個交易日沒有數據。')
            continue

        # 獲取所有價格，並排序
        prices = sorted(grouped_data['price'].unique())

        # 獲取所有可能的買入和賣出的券商
        buy_traders = grouped_data[grouped_data['buy'] > 0]['securities_trader'].unique()
        sell_traders = grouped_data[grouped_data['sell'] > 0]['securities_trader'].unique()

        num_buy_traders = len(buy_traders)
        num_sell_traders = len(sell_traders)

        # 使用顏色映射
        buy_colorscale = px.colors.sequential.Reds
        sell_colorscale = px.colors.sequential.Greens

        buy_colors = sample_colorscale(buy_colorscale, np.linspace(0.6, 0.9, num_buy_traders))
        sell_colors = sample_colorscale(sell_colorscale, np.linspace(0.6, 0.9, num_sell_traders))

        # 創建買入和賣出的顏色映射字典
        buy_color_map = {trader: color for trader, color in zip(buy_traders, buy_colors)}
        sell_color_map = {trader: color for trader, color in zip(sell_traders, sell_colors)}

        # 計算每個價格的總買入和賣出量
        price_total_buy = grouped_data.groupby('price')['buy'].sum().to_dict()
        price_total_sell = grouped_data.groupby('price')['sell'].sum().to_dict()

        # 準備買入和賣出的數據
        buy_data = {}
        sell_data = {}

        # 設置標籤顯示閾值
        LABEL_THRESHOLD = 0.05

        # 獲取前 TOP_N 名買入和賣出量的交易商
        for price in prices:
            price_data = grouped_data[grouped_data['price'] == price]

            # 獲取前 TOP_N 名買入量 > 0 的交易商
            top_buyers_price = price_data[price_data['buy'] > 0].sort_values(by='buy', ascending=False).head(N)

            # 獲取前 TOP_N 名賣出量 > 0 的交易商
            top_sellers_price = price_data[price_data['sell'] > 0].sort_values(by='sell', ascending=False).head(N)

            # 準備買入數據
            for _, row in top_buyers_price.iterrows():
                trader = row['securities_trader']
                buy_volume = row['buy']
                if trader not in buy_data:
                    buy_data[trader] = {'price': [], 'buy': []}
                buy_data[trader]['price'].append(price)
                buy_data[trader]['buy'].append(buy_volume)

            # 準備賣出數據
            for _, row in top_sellers_price.iterrows():
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
                text=f'{current_date} 及前 {cumulative_days -1} 個交易日券商累積買入和賣出量分佈',
                x=0.5,
                xanchor='center',
                font=dict(size=24),
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

        # 在 Streamlit 中顯示圖表
        st.plotly_chart(fig, use_container_width=True)
