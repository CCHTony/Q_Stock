# tabs/tab8.py

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
from plotly.colors import sample_colorscale

def render_tab8(df_period, N):
    st.subheader('淨持有量分佈')
    
    # 定義 min_date 和 max_date
    min_date = df_period['date'].min()
    max_date = df_period['date'].max()

    # 初始化 session_state 中的 'cumulative_entries_tab8' 如果尚未存在
    if 'cumulative_entries_tab8' not in st.session_state:
        st.session_state.cumulative_entries_tab8 = [{'date': None, 'cumulative_days': 3}]

    # 定義一個函數來添加新條目
    def add_entry():
        st.session_state.cumulative_entries_tab8.append({'date': None, 'cumulative_days': 3})

    # 定義一個函數來移除條目
    def remove_entry(index):
        if len(st.session_state.cumulative_entries_tab8) > 1:
            st.session_state.cumulative_entries_tab8.pop(index)

    # 添加按鈕來添加新條目，並指定唯一的 key
    st.button('添加新日期-回朔天數組合', on_click=add_entry, key='add_entry_tab8')

    # 迭代並顯示所有條目
    for idx, entry in enumerate(st.session_state.cumulative_entries_tab8):
        with st.container():
            cols = st.columns([1, 1, 0.2])
            with cols[0]:
                # 日期選擇
                date_key = f'date_tab8_{idx}'
                current_date = st.session_state.cumulative_entries_tab8[idx]['date']
                # 驗證日期是否在範圍內
                if current_date is not None and not (min_date <= current_date <= max_date):
                    st.session_state.cumulative_entries_tab8[idx]['date'] = min_date
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
                days_key = f'days_tab8_{idx}'
                cumulative_days = st.number_input(
                    f'回朔天數 #{idx + 1}',
                    min_value=1,
                    max_value=30,
                    value=st.session_state.cumulative_entries_tab8[idx]['cumulative_days'],
                    step=1,
                    key=days_key
                )
            with cols[2]:
                # 移除條目按鈕，並指定唯一的 key
                if idx != 0:
                    st.button('❌', key=f'remove_tab8_{idx}', on_click=remove_entry, args=(idx,))

            # 更新 session_state
            st.session_state.cumulative_entries_tab8[idx]['date'] = selected_date
            st.session_state.cumulative_entries_tab8[idx]['cumulative_days'] = cumulative_days

    # 選擇交易日期和回朔天數後生成圖表
    st.markdown('---')
    st.write('### 淨持有量分佈圖')

    for idx, entry in enumerate(st.session_state.cumulative_entries_tab8):
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

        # 按價格和券商分組，計算每個券商在每個價格下的買入、賣出量
        grouped_data = cumulative_data.groupby(['price', 'securities_trader']).agg({
            'buy': 'sum',
            'sell': 'sum',
        }).reset_index()

        if grouped_data.empty:
            st.warning(f'在日期 {current_date} 及其前 {cumulative_days -1} 個交易日沒有數據。')
            continue

        # 計算淨持有量（買入 - 賣出）
        grouped_data['net_holdings'] = grouped_data['buy'] - grouped_data['sell']

        # 獲取所有價格，並排序
        prices = sorted(grouped_data['price'].unique())

        # 分離正淨持有量和負淨持有量
        positive_holdings = grouped_data[grouped_data['net_holdings'] > 0]
        negative_holdings = grouped_data[grouped_data['net_holdings'] <= 0]

        # 獲取獨特的券商名稱
        positive_brokers = positive_holdings['securities_trader'].unique()
        negative_brokers = negative_holdings['securities_trader'].unique()

        # 使用不同的顏色映射
        red_colorscale = px.colors.sequential.Reds
        green_colorscale = px.colors.sequential.Greens

        red_colors = sample_colorscale(red_colorscale, np.linspace(0.6, 0.9, len(positive_brokers))) if len(positive_brokers) > 0 else []
        green_colors = sample_colorscale(green_colorscale, np.linspace(0.6, 0.9, len(negative_brokers))) if len(negative_brokers) > 0 else []

        # 創建券商到顏色的映射
        positive_broker_color_map = {broker: color for broker, color in zip(positive_brokers, red_colors)}
        negative_broker_color_map = {broker: color for broker, color in zip(negative_brokers, green_colors)}

        # 準備數據
        positive_holdings_data = {}
        negative_holdings_data = {}

        # 設置標籤顯示閾值
        LABEL_THRESHOLD = 0.05

        # 取前 N 名淨持有量絕對值最大的券商
        for price in prices:
            price_data = grouped_data[grouped_data['price'] == price]

            # 計算淨持有量絕對值最大的前 N 名券商
            top_brokers_price = price_data.reindex(price_data['net_holdings'].abs().sort_values(ascending=False).head(N).index)

            # 分離正負持有量
            top_positive = top_brokers_price[top_brokers_price['net_holdings'] > 0]
            top_negative = top_brokers_price[top_brokers_price['net_holdings'] < 0]

            # 準備正持有量數據
            for _, row in top_positive.iterrows():
                broker = row['securities_trader']
                net_holding = row['net_holdings']
                if broker not in positive_holdings_data:
                    positive_holdings_data[broker] = {'price': [], 'net_holdings': []}
                positive_holdings_data[broker]['price'].append(price)
                positive_holdings_data[broker]['net_holdings'].append(net_holding)

            # 準備負持有量數據
            for _, row in top_negative.iterrows():
                broker = row['securities_trader']
                net_holding = row['net_holdings']
                if broker not in negative_holdings_data:
                    negative_holdings_data[broker] = {'price': [], 'net_holdings': []}
                negative_holdings_data[broker]['price'].append(price)
                negative_holdings_data[broker]['net_holdings'].append(net_holding)

        # 初始化繪圖
        fig = go.Figure()

        # 繪製正淨持有量的堆疊橫向柱狀圖（紅色系）
        for broker, data_dict in positive_holdings_data.items():
            net_holdings = data_dict['net_holdings']
            prices_list = data_dict['price']
            texts = []
            for net_holding in net_holdings:
                text = f"{broker}: {net_holding}"
                texts.append(text)
            fig.add_trace(go.Bar(
                x=net_holdings,
                y=prices_list,
                name=f'{broker}',
                orientation='h',
                marker_color=positive_broker_color_map.get(broker, '#FF0000'),  # 默認紅色
                text=texts,
                textposition='inside',
                textfont=dict(color='white', size=9),
                hovertemplate='%{y}: %{x}',
            ))

        # 繪製負淨持有量的堆疊橫向柱狀圖（綠色系）
        for broker, data_dict in negative_holdings_data.items():
            net_holdings = data_dict['net_holdings']
            prices_list = data_dict['price']
            texts = []
            for net_holding in net_holdings:
                text = f"{broker}: {net_holding}"
                texts.append(text)
            fig.add_trace(go.Bar(
                x=net_holdings,
                y=prices_list,
                name=f'{broker}',
                orientation='h',
                marker_color=negative_broker_color_map.get(broker, '#006400'),  # 默認深綠色
                text=texts,
                textposition='inside',
                textfont=dict(color='white', size=9),
                hovertemplate='%{y}: %{x}',
            ))

        # 設置圖表布局
        fig.update_layout(
            barmode='relative',
            title=dict(
                text=f'{current_date} 及前 {cumulative_days -1} 個交易日券商累積淨持有量分佈',
                x=0.5,
                xanchor='center',
                font=dict(size=24),
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
            legend_title_text='券商',
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
