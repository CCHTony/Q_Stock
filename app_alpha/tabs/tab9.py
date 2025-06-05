# tabs/tab9.py

import streamlit as st
import plotly.express as px
import pandas as pd

def render_tab9(df_period, N):
    st.subheader('價格持有量強弱分析')

    # 定義 min_date 和 max_date
    min_date = df_period['date'].min()
    max_date = df_period['date'].max()

    # 初始化 session_state 中的 'cumulative_entries_tab9' 如果尚未存在
    if 'cumulative_entries_tab9' not in st.session_state:
        st.session_state.cumulative_entries_tab9 = [{'date': None, 'cumulative_days': 3}]

    # 定義一個函數來添加新條目
    def add_entry():
        st.session_state.cumulative_entries_tab9.append({'date': None, 'cumulative_days': 3})

    # 定義一個函數來移除條目
    def remove_entry(index):
        if len(st.session_state.cumulative_entries_tab9) > 1:
            st.session_state.cumulative_entries_tab9.pop(index)

    # 添加按鈕來添加新條目，並指定唯一的 key
    st.button('添加新日期-回朔天數組合', on_click=add_entry, key='add_entry_tab9')

    # 迭代並顯示所有條目
    for idx, entry in enumerate(st.session_state.cumulative_entries_tab9):
        with st.container():
            cols = st.columns([1, 1, 0.2])
            with cols[0]:
                # 日期選擇
                date_key = f'date_tab9_{idx}'
                current_date = st.session_state.cumulative_entries_tab9[idx]['date']
                # 驗證日期是否在範圍內
                if current_date is not None and not (min_date <= current_date <= max_date):
                    st.session_state.cumulative_entries_tab9[idx]['date'] = min_date
                    current_date = max_date
                selected_date = st.date_input(
                    f'選擇日期 #{idx + 1}',
                    value=current_date or max_date,
                    min_value=min_date,
                    max_value=max_date,
                    key=date_key
                )
            with cols[1]:
                # 回朔天數
                days_key = f'days_tab9_{idx}'
                cumulative_days = st.number_input(
                    f'回朔天數 #{idx + 1}',
                    min_value=1,
                    max_value=30,
                    value=st.session_state.cumulative_entries_tab9[idx]['cumulative_days'],
                    step=1,
                    key=days_key
                )
            with cols[2]:
                # 移除條目按鈕，並指定唯一的 key
                if idx != 0:
                    st.button('❌', key=f'remove_tab9_{idx}', on_click=remove_entry, args=(idx,))

            # 更新 session_state
            st.session_state.cumulative_entries_tab9[idx]['date'] = selected_date
            st.session_state.cumulative_entries_tab9[idx]['cumulative_days'] = cumulative_days

    # 選擇交易日期和回朔天數後生成圖表
    st.markdown('---')
    st.write('### 價格持有量強弱圖')

    for idx, entry in enumerate(st.session_state.cumulative_entries_tab9):
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

        if cumulative_data.empty:
            st.warning(f'在日期 {current_date} 及其前 {cumulative_days -1} 個交易日沒有數據。')
            continue

        # 計算每個券商在每個價格的淨持有量
        cumulative_data['net_holdings'] = cumulative_data['buy'] - cumulative_data['sell']

        # 按價格分組
        grouped_by_price = cumulative_data.groupby('price')

        # 準備存儲結果的列表
        results = []

        for price, group in grouped_by_price:
            # 按券商分組，計算淨持有量
            broker_holdings = group.groupby('securities_trader')['net_holdings'].sum().reset_index()

            # 排序得到前 N 大買超券商和前 N 大賣超券商
            top_buyers = broker_holdings.sort_values(by='net_holdings', ascending=False).head(N)
            top_sellers = broker_holdings.sort_values(by='net_holdings', ascending=True).head(N)

            # 計算該價格的淨持有量
            net_holding = top_buyers['net_holdings'].sum() + top_sellers['net_holdings'].sum()

            # 將結果添加到列表
            results.append({
                'price': price,
                'net_holding': net_holding,
                'holding_type': '正值' if net_holding >= 0 else '負值',
                'top_buyers': top_buyers,
                'top_sellers': top_sellers
            })

        # 將結果轉換為 DataFrame
        results_df = pd.DataFrame(results)

        if results_df.empty:
            st.warning(f'在日期 {current_date} 及其前 {cumulative_days -1} 個交易日沒有有效數據。')
            continue

        # 按價格排序
        results_df = results_df.sort_values(by='price', ascending=True)

        # 繪製圖表
        fig = px.bar(
            results_df,
            x='price',
            y='net_holding',
            color='holding_type',
            labels={'price': '價格', 'net_holding': '淨持有量'},
            title=f'{current_date} 及前 {cumulative_days -1} 個交易日價格持有量強弱圖',
            color_discrete_map={'正值': 'red', '負值': 'green'}
        )

        st.plotly_chart(fig, use_container_width=True)

        # # 顯示詳細信息
        # st.write(f'#### {current_date} 及前 {cumulative_days -1} 個交易日價格持有量詳情')

        # for result in results:
        #     price = result['price']
        #     net_holding = result['net_holding']
        #     top_buyers = result['top_buyers']
        #     top_sellers = result['top_sellers']

        #     st.write(f'**價格：{price}**')
        #     st.write(f'淨持有量：{net_holding}')

        #     # 顯示前 N 大買超券商
        #     st.write('前 N 大買超券商：')
        #     st.table(top_buyers[['securities_trader', 'net_holdings']])

        #     # 顯示前 N 大賣超券商
        #     st.write('前 N 大賣超券商：')
        #     st.table(top_sellers[['securities_trader', 'net_holdings']])

        #     st.markdown('---')
