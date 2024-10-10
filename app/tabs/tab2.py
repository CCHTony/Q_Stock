# tabs/tab2.py

import streamlit as st
import plotly.express as px
import pandas as pd

def render_tab2(df_period, N):
    st.subheader('大賣家')

    # 初始化 session_state 中的自訂時間段
    if 'custom_periods_input_tab2' not in st.session_state or st.session_state.stock_code != st.session_state.get('stock_code', ''):
        st.session_state.custom_periods_input_tab2 = '3,5,10,22'

    custom_periods_input = st.text_input(
        '輸入要查看的近幾天（用逗號分隔，例如：3,5,10）',
        st.session_state.custom_periods_input_tab2,
        key='custom_periods_tab2'
    )
    st.session_state.custom_periods_input_tab2 = custom_periods_input
    
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

            grouped = df_recent.groupby('securities_trader').agg({'sell': 'sum'}).reset_index()
            top_sellers = grouped.sort_values(by='sell', ascending=False).head(N)

            fig_sellers = px.bar(
                top_sellers,
                x='securities_trader',
                y='sell',
                labels={'sell': '賣出量', 'securities_trader': '券商'},
                title=f'近 {days} 天大賣家'
            )
            st.plotly_chart(fig_sellers, use_container_width=True)

    # 显示全部数据
    st.markdown('### 全部')
    grouped_all = df_period.groupby('securities_trader').agg({'sell': 'sum'}).reset_index()
    top_sellers_all = grouped_all.sort_values(by='sell', ascending=False).head(N)

    fig_sellers_all = px.bar(
        top_sellers_all,
        x='securities_trader',
        y='sell',
        labels={'sell': '賣出量', 'securities_trader': '券商'},
        title='全部大賣家'
    )
    st.plotly_chart(fig_sellers_all, use_container_width=True)
