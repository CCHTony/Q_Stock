# tabs/tab5.py

import streamlit as st
import plotly.express as px

def render_tab5(df_period, selected_buy_traders, selected_sell_traders):
    st.subheader('持有量變化')
    
    selected_traders = selected_buy_traders + selected_sell_traders

    # 只保留選定的券商資料
    df_top = df_period[df_period['securities_trader'].isin(selected_traders)]

    # 計算每日買入量和賣出量
    daily_totals = df_top.groupby(['securities_trader', 'date']).agg({'buy': 'sum', 'sell': 'sum'}).reset_index()

    # 按日期排序
    daily_totals = daily_totals.sort_values(by=['securities_trader', 'date'])

    # 計算累計買入量和累計賣出量
    daily_totals['cumulative_buy'] = daily_totals.groupby('securities_trader')['buy'].cumsum()
    daily_totals['cumulative_sell'] = daily_totals.groupby('securities_trader')['sell'].cumsum()

    # 計算累計買超量（買入量 - 賣出量）
    daily_totals['cumulative_buy_super'] = daily_totals['cumulative_buy'] - daily_totals['cumulative_sell']

    # 繪製累計買超量的折線圖（直接呈現正負值）
    fig_buy_super = px.line(
        daily_totals,
        x='date',
        y='cumulative_buy_super',
        color='securities_trader',
        labels={
            'cumulative_buy_super': '累計買超量',
            'date': '日期',
            'securities_trader': '券商'
        },
        title='累計買超量隨時間的變化（正值為買超，負值為賣超）'
    )
    st.plotly_chart(fig_buy_super, use_container_width=True)
