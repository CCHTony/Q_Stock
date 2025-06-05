# tabs/tab7.py

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
from plotly.colors import sample_colorscale
import pandas as pd

def render_tab7(df_period, selected_track_traders):
    st.subheader('追蹤特定券商')

    if not selected_track_traders:
        st.warning('請選擇至少一個券商來追蹤。')
        return

    # 定義 min_date 和 max_date
    min_date = pd.to_datetime(df_period['date']).min()
    max_date = pd.to_datetime(df_period['date']).max()

    # 初始化 session_state 中的 'track_entries' 如果尚未存在
    if 'track_entries' not in st.session_state:
        st.session_state.track_entries = [{
            'mode': 'days',
            'specific_date': None,
            'specific_cumulative_days': 3,
            'overall_date': None,
            'overall_cumulative_days': 3
        }]

    # 定義一個函數來添加新條目
    def add_track_entry():
        st.session_state.track_entries.append({
            'mode': 'days',
            'specific_date': None,
            'specific_cumulative_days': 3,
            'overall_date': None,
            'overall_cumulative_days': 3
        })

    # 定義一個函數來移除條目
    def remove_track_entry(index):
        if len(st.session_state.track_entries) > 1:
            st.session_state.track_entries.pop(index)

    # 添加按鈕來添加新條目，並指定唯一的 key
    st.button('添加新日期-回朔天數組合', on_click=add_track_entry, key='add_entry_tab7')

    # 迭代並顯示所有條目
    for idx, entry in enumerate(st.session_state.track_entries):
        with st.container():
            # 調整佈局：將選擇模式和日期選擇放在同一行
            cols = st.columns([1, 1, 1, 1, 0.2])
            with cols[0]:
                # 模式選擇：回溯天數或日期範圍
                mode_key = f'mode_tab7_{idx}'
                mode = st.selectbox(
                    f'選擇模式 #{idx + 1}',
                    options=['回溯天數', '日期範圍'],
                    index=0 if entry['mode'] == 'days' else 1,
                    key=mode_key
                )
                st.session_state.track_entries[idx]['mode'] = 'days' if mode == '回溯天數' else 'range'

            if st.session_state.track_entries[idx]['mode'] == 'days':
                with cols[1]:
                    # 特定券商的日期選擇
                    specific_date_key = f'track_specific_date_{idx}'
                    current_specific_date = st.session_state.track_entries[idx]['specific_date']
                    # 驗證日期是否在範圍內
                    if current_specific_date is not None and not (min_date <= pd.to_datetime(current_specific_date) <= max_date):
                        st.session_state.track_entries[idx]['specific_date'] = min_date
                        current_specific_date = min_date
                    selected_specific_date = st.date_input(
                        f'券商日期 #{idx + 1}',
                        value=pd.to_datetime(current_specific_date) if current_specific_date else min_date,
                        min_value=min_date,
                        max_value=max_date,
                        key=f'specific_{specific_date_key}'
                    )
                with cols[2]:
                    # 特定券商的回溯天數
                    specific_days_key = f'track_specific_days_{idx}'
                    specific_cumulative_days = st.number_input(
                        f'券商回溯天數 #{idx + 1}',
                        min_value=1,
                        max_value=30,
                        value=st.session_state.track_entries[idx].get('specific_cumulative_days', 3),
                        step=1,
                        key=specific_days_key
                    )
                with cols[3]:
                    # 整體市場的日期選擇
                    overall_date_key = f'track_overall_date_{idx}'
                    current_overall_date = st.session_state.track_entries[idx]['overall_date']
                    # 驗證日期是否在範圍內
                    if current_overall_date is not None and not (min_date <= pd.to_datetime(current_overall_date) <= max_date):
                        st.session_state.track_entries[idx]['overall_date'] = min_date
                        current_overall_date = min_date
                    selected_overall_date = st.date_input(
                        f'整體日期 #{idx + 1}',
                        value=pd.to_datetime(current_overall_date) if current_overall_date else min_date,
                        min_value=min_date,
                        max_value=max_date,
                        key=f'overall_{overall_date_key}'
                    )
                with cols[4]:
                    # 整體市場的回溯天數
                    overall_days_key = f'track_overall_days_{idx}'
                    overall_cumulative_days = st.number_input(
                        f'整體回溯天數 #{idx + 1}',
                        min_value=1,
                        max_value=30,
                        value=st.session_state.track_entries[idx].get('overall_cumulative_days', 3),
                        step=1,
                        key=overall_days_key
                    )
            elif st.session_state.track_entries[idx]['mode'] == 'range':
                with cols[1]:
                    # 特定券商的開始日期選擇
                    specific_start_date_key = f'track_specific_start_date_{idx}'
                    current_specific_start_date = st.session_state.track_entries[idx].get('specific_start_date', min_date)
                    selected_specific_start_date = st.date_input(
                        f'券商開始日期 #{idx + 1}',
                        value=pd.to_datetime(current_specific_start_date) if current_specific_start_date else min_date,
                        min_value=min_date,
                        max_value=max_date,
                        key=f'specific_start_{specific_start_date_key}'
                    )
                with cols[2]:
                    # 特定券商的結束日期選擇
                    specific_end_date_key = f'track_specific_end_date_{idx}'
                    current_specific_end_date = st.session_state.track_entries[idx].get('specific_end_date', max_date)
                    selected_specific_end_date = st.date_input(
                        f'券商結束日期 #{idx + 1}',
                        value=pd.to_datetime(current_specific_end_date) if current_specific_end_date else max_date,
                        min_value=min_date,
                        max_value=max_date,
                        key=f'specific_end_{specific_end_date_key}'
                    )
                with cols[3]:
                    # 整體市場的開始日期選擇
                    overall_start_date_key = f'track_overall_start_date_{idx}'
                    current_overall_start_date = st.session_state.track_entries[idx].get('overall_start_date', min_date)
                    selected_overall_start_date = st.date_input(
                        f'整體開始日期 #{idx + 1}',
                        value=pd.to_datetime(current_overall_start_date) if current_overall_start_date else min_date,
                        min_value=min_date,
                        max_value=max_date,
                        key=f'overall_start_{overall_start_date_key}'
                    )
                with cols[4]:
                    # 整體市場的結束日期選擇
                    overall_end_date_key = f'track_overall_end_date_{idx}'
                    current_overall_end_date = st.session_state.track_entries[idx].get('overall_end_date', max_date)
                    selected_overall_end_date = st.date_input(
                        f'整體結束日期 #{idx + 1}',
                        value=pd.to_datetime(current_overall_end_date) if current_overall_end_date else max_date,
                        min_value=min_date,
                        max_value=max_date,
                        key=f'overall_end_{overall_end_date_key}'
                    )

                # 整體市場的回溯天數在範圍模式下不適用
                overall_cumulative_days = st.session_state.track_entries[idx].get('overall_cumulative_days', 3)
            
            with cols[4]:
                # 移除條目按鈕，並指定唯一的 key
                if idx != 0:
                    st.button('❌', key=f'remove_tab7_{idx}', on_click=remove_track_entry, args=(idx,))

            # 更新 session_state
            if st.session_state.track_entries[idx]['mode'] == 'days':
                st.session_state.track_entries[idx]['specific_date'] = selected_specific_date
                st.session_state.track_entries[idx]['specific_cumulative_days'] = specific_cumulative_days
                st.session_state.track_entries[idx]['overall_date'] = selected_overall_date
                st.session_state.track_entries[idx]['overall_cumulative_days'] = overall_cumulative_days
            elif st.session_state.track_entries[idx]['mode'] == 'range':
                st.session_state.track_entries[idx]['specific_start_date'] = selected_specific_start_date
                st.session_state.track_entries[idx]['specific_end_date'] = selected_specific_end_date
                st.session_state.track_entries[idx]['overall_start_date'] = selected_overall_start_date
                st.session_state.track_entries[idx]['overall_end_date'] = selected_overall_end_date

    # 選擇交易日期和回朔天數後生成圖表
    st.markdown('---')
    st.write('### 追蹤券商交易量分佈圖')

    for idx, entry in enumerate(st.session_state.track_entries):
        if entry['mode'] == 'days':
            selected_date = entry['specific_date']
            cumulative_days = entry['specific_cumulative_days']
            overall_selected_date = entry['overall_date']
            overall_cumulative_days = entry['overall_cumulative_days']

            if selected_date is None or overall_selected_date is None:
                st.warning(f'請選擇日期 #{idx + 1}。')
                continue

            # 將選定日期轉換為 datetime 格式
            current_specific_date = pd.to_datetime(selected_date)
            current_overall_date = pd.to_datetime(overall_selected_date)

            # 獲取所有唯一且排序的交易日期
            all_dates = sorted(pd.to_datetime(df_period['date'].unique()))
            try:
                current_specific_index = all_dates.index(current_specific_date)
            except ValueError:
                st.warning(f'選定的券商日期 {current_specific_date.date()} 不存在於資料中。')
                continue
            try:
                current_overall_index = all_dates.index(current_overall_date)
            except ValueError:
                st.warning(f'選定的整體日期 {current_overall_date.date()} 不存在於資料中。')
                continue

            # 獲取特定券商的累積日期
            if cumulative_days > len(all_dates):
                st.warning(f'券商回溯天數 {cumulative_days} 超過可用的交易日數（最多 {len(all_dates)} 天）。將選取所有可用的交易日。')
                specific_selected_dates = all_dates
            else:
                specific_selected_dates = all_dates[max(0, current_specific_index - (cumulative_days - 1)): current_specific_index + 1]

            # 獲取整體市場的累積日期
            if overall_cumulative_days > len(all_dates):
                st.warning(f'整體回溯天數 {overall_cumulative_days} 超過可用的交易日數（最多 {len(all_dates)} 天）。將選取所有可用的交易日。')
                overall_selected_dates = all_dates
            else:
                overall_selected_dates = all_dates[max(0, current_overall_index - (overall_cumulative_days - 1)): current_overall_index + 1]

            # 篩選特定券商的數據
            cumulative_data_specific = df_period[
                (pd.to_datetime(df_period['date']).isin(specific_selected_dates)) &
                (df_period['securities_trader'].isin(selected_track_traders))
            ]

            # 篩選整體市場的數據
            cumulative_data_all = df_period[pd.to_datetime(df_period['date']).isin(overall_selected_dates)]

            # 檢查數據是否存在
            if cumulative_data_specific.empty:
                st.warning(f'在券商日期 {current_specific_date.date()} 及其前 {cumulative_days -1} 個交易日沒有特定券商的數據。')
                continue

            if cumulative_data_all.empty:
                st.warning(f'在整體日期 {current_overall_date.date()} 及其前 {overall_cumulative_days -1} 個交易日沒有整體數據。')
                continue

            # 按價格和券商分組特定券商的數據
            grouped_specific = cumulative_data_specific.groupby(['price', 'securities_trader']).agg({
                'buy': 'sum',
                'sell': 'sum',
            }).reset_index()

            # 按價格分組整體市場的數據
            grouped_all = cumulative_data_all.groupby('price').agg({'buy': 'sum', 'sell': 'sum'}).reset_index()

            # 獲取所有價格，並排序
            prices = sorted(set(grouped_specific['price'].unique()).union(grouped_all['price'].unique()))

            # 獲取特定券商的買入和賣出交易商
            buy_traders = grouped_specific[grouped_specific['buy'] > 0]['securities_trader'].unique()
            sell_traders = grouped_specific[grouped_specific['sell'] > 0]['securities_trader'].unique()

            num_buy_traders = len(buy_traders)
            num_sell_traders = len(sell_traders)

            # 使用顏色映射
            buy_colorscale = px.colors.sequential.Reds
            sell_colorscale = px.colors.sequential.Greens

            buy_colors = sample_colorscale(buy_colorscale, np.linspace(0.6, 0.9, num_buy_traders)) if num_buy_traders > 0 else []
            sell_colors = sample_colorscale(sell_colorscale, np.linspace(0.6, 0.9, num_sell_traders)) if num_sell_traders > 0 else []

            # 創建買入和賣出的顏色映射字典
            buy_color_map = {trader: color for trader, color in zip(buy_traders, buy_colors)}
            sell_color_map = {trader: color for trader, color in zip(sell_traders, sell_colors)}

            # 計算每個價格的總買入和賣出量（特定券商）
            price_total_buy_specific = grouped_specific.groupby('price')['buy'].sum().to_dict()
            price_total_sell_specific = grouped_specific.groupby('price')['sell'].sum().to_dict()

            # 準備買入和賣出的數據（特定券商）
            buy_data_specific = {}
            sell_data_specific = {}

            # 設置標籤顯示閾值
            LABEL_THRESHOLD = 0.05

            # 準備特定券商的數據
            for price in prices:
                price_data = grouped_specific[grouped_specific['price'] == price]

                # 買入
                top_buyers_price = price_data[price_data['buy'] > 0].sort_values(by='buy', ascending=False)
                for _, row in top_buyers_price.iterrows():
                    trader = row['securities_trader']
                    buy_volume = row['buy']
                    if trader not in buy_data_specific:
                        buy_data_specific[trader] = {'price': [], 'buy': []}
                    buy_data_specific[trader]['price'].append(price)
                    buy_data_specific[trader]['buy'].append(buy_volume)

                # 賣出
                top_sellers_price = price_data[price_data['sell'] > 0].sort_values(by='sell', ascending=False)
                for _, row in top_sellers_price.iterrows():
                    trader = row['securities_trader']
                    sell_volume = row['sell']
                    if trader not in sell_data_specific:
                        sell_data_specific[trader] = {'price': [], 'sell': []}
                    sell_data_specific[trader]['price'].append(price)
                    sell_data_specific[trader]['sell'].append(sell_volume)

            # 計算每個價格的總買入和賣出量（整體市場）
            price_total_buy_all = grouped_all.groupby('price')['buy'].sum().to_dict()
            price_total_sell_all = grouped_all.groupby('price')['sell'].sum().to_dict()

            # 準備整體市場的買入和賣出數據
            buy_data_all = {price: price_total_buy_all.get(price, 0) for price in prices}
            sell_data_all = {price: price_total_sell_all.get(price, 0) for price in prices}

            # 初始化繪圖
            fig = go.Figure()

            # 繪製整體買入的堆疊橫向柱狀圖（背景，半透明）
            fig.add_trace(go.Bar(
                x=[buy_data_all.get(price, 0) for price in prices],
                y=prices,
                name='總買入',
                orientation='h',
                marker_color='rgba(255, 0, 0, 0.3)',  # 半透明紅色
                legendgroup='all_buy',
                showlegend=True,
                hovertemplate='總買入<br>%{y}: %{x}',
            ))

            # 繪製整體賣出的堆疊橫向柱狀圖（背景，半透明）
            fig.add_trace(go.Bar(
                x=[-sell_data_all.get(price, 0) for price in prices],
                y=prices,
                name='總賣出',
                orientation='h',
                marker_color='rgba(0, 128, 0, 0.3)',  # 半透明綠色
                legendgroup='all_sell',
                showlegend=True,
                hovertemplate='總賣出<br>%{y}: %{x}',
            ))

            # 繪製特定券商的買入的堆疊橫向柱狀圖
            for trader, data_dict in buy_data_specific.items():
                buy_volumes = data_dict['buy']
                prices_list = data_dict['price']
                texts = []
                for buy_volume, price in zip(buy_volumes, prices_list):
                    total_buy_volume = price_total_buy_specific.get(price, 0)
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

            # 繪製特定券商的賣出的堆疊橫向柱狀圖
            for trader, data_dict in sell_data_specific.items():
                sell_volumes = data_dict['sell']
                prices_list = data_dict['price']
                texts = []
                for sell_volume, price in zip(sell_volumes, prices_list):
                    total_sell_volume = price_total_sell_specific.get(price, 0)
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
                    text=f'券商日期 {current_specific_date.date()}（回溯 {cumulative_days} 天） vs 整體日期 {current_overall_date.date()}（回溯 {overall_cumulative_days} 天）',
                    x=0.5,
                    xanchor='center',
                    font=dict(size=16),
                ),
                xaxis_title='交易量',
                yaxis_title='價格',
                yaxis=dict(
                    categoryorder='category descending',
                    autorange='reversed',
                    titlefont=dict(size=12),
                    tickfont=dict(size=10),
                ),
                xaxis=dict(
                    titlefont=dict(size=12),
                    tickfont=dict(size=10),
                    gridcolor='lightgrey',
                    zeroline=True,
                    zerolinewidth=1,
                    zerolinecolor='grey',
                ),
                legend_title_text='券商',
                legend=dict(
                    font=dict(size=10),
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
                    size=10,
                ),
            )

            # 在 Streamlit 中顯示圖表
            st.plotly_chart(fig, use_container_width=True)
