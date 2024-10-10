# tabs/tab4.py

import streamlit as st
import plotly.express as px
import pandas as pd

def render_tab4(df_period, N):
    st.subheader('賣超')

    # 初始化 session_state 中的自訂時間段
    if 'custom_periods_input_tab4' not in st.session_state or st.session_state.stock_code != st.session_state.get('stock_code', ''):
        st.session_state.custom_periods_input_tab4 = '3,5,10,22'

    custom_periods_input = st.text_input(
        '輸入要查看的近幾天（用逗號分隔，例如：3,5,10）',
        st.session_state.custom_periods_input_tab4,
        key='custom_periods_tab4'
    )
    st.session_state.custom_periods_input_tab4 = custom_periods_input
    
    custom_periods = [int(x.strip()) for x in custom_periods_input.split(',') if x.strip().isdigit()]
    custom_periods = sorted(set(custom_periods))  # 去重并排序

    latest_date = pd.to_datetime(df_period['date'].max())

    for days in custom_periods:
        start_date = latest_date - pd.Timedelta(days=days - 1)  # 包含當天
        mask = (pd.to_datetime(df_period['date']) >= start_date) & (pd.to_datetime(df_period['date']) <= latest_date)
        df_recent = df_period.loc[mask]

        if df_recent.empty:
            st.warning(f'近 {days} 天內沒有資料。')
        else:
            st.markdown(f'### 近 {days} 天')

            grouped = df_recent.groupby('securities_trader').agg({'buy': 'sum', 'sell': 'sum'}).reset_index()
            grouped['buy_super'] = grouped['buy'] - grouped['sell']
            bottom_sell_super = grouped.sort_values(by='buy_super', ascending=True).head(N)

            fig_sell_super = px.bar(
                bottom_sell_super,
                x='securities_trader',
                y='buy_super',
                labels={'buy_super': '賣超量', 'securities_trader': '券商'},
                title=f'近 {days} 天賣超'
            )
            st.plotly_chart(fig_sell_super, use_container_width=True)

    # 显示全部数据
    st.markdown('### 全部')
    grouped_all = df_period.groupby('securities_trader').agg({'buy': 'sum', 'sell': 'sum'}).reset_index()
    grouped_all['buy_super'] = grouped_all['buy'] - grouped_all['sell']
    bottom_sell_super_all = grouped_all.sort_values(by='buy_super', ascending=True).head(N)

    fig_sell_super_all = px.bar(
        bottom_sell_super_all,
        x='securities_trader',
        y='buy_super',
        labels={'buy_super': '賣超量', 'securities_trader': '券商'},
        title='全部賣超'
    )
    st.plotly_chart(fig_sell_super_all, use_container_width=True)
