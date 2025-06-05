# tabs/tab4.py

import streamlit as st
import plotly.graph_objects as go
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
    custom_periods = sorted(set(custom_periods))  # 去重並排序

    # 獲取所有唯一且排序的交易日期
    all_dates = sorted(pd.to_datetime(df_period['date'].unique()))
    latest_date = all_dates[-1]

    for days in custom_periods:
        if days <= 0:
            st.warning(f'天數 {days} 必須大於 0。')
            continue

        # 確保選取不超過可用的交易日數
        if days > len(all_dates):
            st.warning(f'天數 {days} 超過可用的交易日數（最多 {len(all_dates)} 天）。將選取所有可用的交易日。')
            selected_dates = all_dates
        else:
            # 選取最後 'days' 個交易日
            selected_dates = all_dates[-days:]

        # 篩選選取的交易日數據
        df_recent = df_period[pd.to_datetime(df_period['date']).isin(selected_dates)]

        if df_recent.empty:
            st.warning(f'近 {days} 天內沒有資料。')
            continue
        else:
            st.markdown(f'### 近 {days} 天')

            # 計算整個期間的買賣超
            grouped_total = df_recent.groupby('securities_trader').agg({'buy': 'sum', 'sell': 'sum'}).reset_index()
            grouped_total['buy_super_total'] = grouped_total['buy'] - grouped_total['sell']

            # 定義子期間：最近一天和其他天數
            if days == 1:
                grouped_recent = grouped_total.copy()
                grouped_others = pd.DataFrame(columns=grouped_total.columns)
                grouped_others['buy_super_others'] = 0
            else:
                # 最近一天的數據
                df_last_day = df_recent[pd.to_datetime(df_recent['date']) == latest_date]
                grouped_recent = df_last_day.groupby('securities_trader').agg({'buy': 'sum', 'sell': 'sum'}).reset_index()
                grouped_recent['buy_super_recent'] = grouped_recent['buy'] - grouped_recent['sell']

                # 其他天數的數據
                other_dates = [date for date in selected_dates if date != latest_date]
                df_other_days = df_recent[pd.to_datetime(df_recent['date']).isin(other_dates)]
                grouped_others = df_other_days.groupby('securities_trader').agg({'buy': 'sum', 'sell': 'sum'}).reset_index()
                grouped_others['buy_super_others'] = grouped_others['buy'] - grouped_others['sell']

            # 合併數據
            merged = pd.merge(
                grouped_total[['securities_trader', 'buy_super_total']],
                grouped_recent[['securities_trader', 'buy_super_recent']],
                on='securities_trader',
                how='left'
            )
            merged = pd.merge(
                merged,
                grouped_others[['securities_trader', 'buy_super_others']],
                on='securities_trader',
                how='left'
            )

            # 填充缺失值為 0
            merged[['buy_super_recent', 'buy_super_others']] = merged[['buy_super_recent', 'buy_super_others']].fillna(0)

            # 選取賣超前 N 名的券商
            bottom_sell_super = merged.sort_values(by='buy_super_total', ascending=True).head(N)

            # 分離正負值
            bottom_sell_super['recent_positive'] = bottom_sell_super['buy_super_recent'].apply(lambda x: x if x >= 0 else 0)
            bottom_sell_super['recent_negative'] = bottom_sell_super['buy_super_recent'].apply(lambda x: x if x < 0 else 0)
            bottom_sell_super['others_positive'] = bottom_sell_super['buy_super_others'].apply(lambda x: x if x >= 0 else 0)
            bottom_sell_super['others_negative'] = bottom_sell_super['buy_super_others'].apply(lambda x: x if x < 0 else 0)

            # 繪製圖表
            fig = go.Figure()

            # 添加正值部分（堆疊在零軸以上）
            fig.add_trace(go.Bar(
                x=bottom_sell_super['securities_trader'],
                y=bottom_sell_super['others_positive'],
                name=f'其他 {days - 1} 天（買超）',
                marker_color='lightcoral'
            ))
            fig.add_trace(go.Bar(
                x=bottom_sell_super['securities_trader'],
                y=bottom_sell_super['recent_positive'],
                name='最近一天（買超）',
                marker_color='red'
            ))

            # 添加負值部分（堆疊在零軸以下）
            fig.add_trace(go.Bar(
                x=bottom_sell_super['securities_trader'],
                y=bottom_sell_super['others_negative'],
                name=f'其他 {days - 1} 天（賣超）',
                marker_color='lightgreen'
            ))
            fig.add_trace(go.Bar(
                x=bottom_sell_super['securities_trader'],
                y=bottom_sell_super['recent_negative'],
                name='最近一天（賣超）',
                marker_color='green'
            ))

            # 更新圖表布局
            fig.update_layout(
                barmode='relative',
                title=f'近 {days} 天賣超（正負堆疊圖）',
                xaxis_title='券商',
                yaxis_title='買賣超量',
                legend_title='期間',
                xaxis={'categoryorder':'total ascending'},
            )

            st.plotly_chart(fig, use_container_width=True)
