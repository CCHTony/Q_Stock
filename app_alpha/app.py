# app.py

import pandas as pd
import streamlit as st
import os

from tabs import tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9

# 設定頁面配置
st.set_page_config(page_title='股票交易分析', layout='wide')

# 標題
st.title('股票交易前 N 大買家和賣家分析')

# 初始化 session_state 中的股票代碼
if 'stock_code' not in st.session_state:
    st.session_state.stock_code = '6405'

# 輸入股票代碼
stock_code = st.text_input('輸入股票代碼', st.session_state.stock_code)
# 檢查是否更改了股票代碼
stock_code_changed = (stock_code != st.session_state.stock_code)
st.session_state.stock_code = stock_code  # 更新 session_state

# 檢查檔案是否存在
file_path = f'brokerDataSet/{stock_code}.csv'
if not os.path.exists(file_path):
    st.error(f'檔案 {file_path} 不存在，請確認股票代碼是否正確。')
else:
    # 讀取 CSV 檔案
    df = pd.read_csv(file_path)

    # 確認必要欄位存在
    required_columns = {'securities_trader', 'buy', 'sell', 'date', 'price'}
    if not required_columns.issubset(df.columns):
        st.error(f'CSV 檔案缺少必要欄位：{required_columns - set(df.columns)}')
    else:
        # 將 'date' 列轉換為日期格式
        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date  # 轉換為日期格式（不含時間）

        # 檢查日期轉換是否成功
        if df['date'].isnull().any():
            st.error('日期欄位存在無法解析的值，請確認日期格式是否正確（應為 YYYY-MM-DD）。')
        else:
            # 選擇日期範圍
            min_date = df['date'].min()
            max_date = df['date'].max()
            
            # 初始化 session_state 中的日期範圍
            if 'date_range' not in st.session_state or st.session_state.stock_code != stock_code:
                st.session_state.date_range = [min_date, max_date]
            
            start_date, end_date = st.date_input(
                '選擇日期範圍',
                st.session_state.date_range,
                min_value=min_date,
                max_value=max_date
            )
            st.session_state.date_range = [start_date, end_date]  # 更新 session_state
            
            # 過濾出選定時間區間內的資料
            mask = (df['date'] >= start_date) & (df['date'] <= end_date)
            df_period = df.loc[mask]

            if df_period.empty:
                st.warning('在選定的日期範圍內沒有資料。')
            else:
                # 設定要查看的前 N 大
                if 'N' not in st.session_state or st.session_state.stock_code != stock_code:
                    st.session_state.N = 20

                N = st.number_input(
                    '輸入要查看的前 N 大買家和賣家',
                    min_value=1,
                    max_value=1000,
                    value=st.session_state.N,
                    step=1
                )
                st.session_state.N = N  # 更新 session_state

                # 佈局選項卡，包括新增的 tab7
                tab1_, tab2_, tab3_, tab4_, tab5_, tab6_, tab7_, tab8_, tab9_ = st.tabs(['大買家', '大賣家', '買超', '賣超', '持有量變化', '交易量分佈', '追蹤特定券商', '淨持有量分佈', '價格持有量強弱分析'])

                with tab1_:
                    tab1.render_tab1(df_period, N)

                with tab2_:
                    tab2.render_tab2(df_period, N)

                with tab3_:
                    tab3.render_tab3(df_period, N)

                with tab4_:
                    tab4.render_tab4(df_period, N)
                    
                with tab5_:
                    # 根據 'securities_trader' 分組，計算買入和賣出總量
                    grouped = df_period.groupby('securities_trader').agg({'buy': 'sum', 'sell': 'sum'}).reset_index()
                    # 計算買超量
                    grouped['buy_super'] = grouped['buy'] - grouped['sell']
                    # 前 N 大買超
                    top_buy_super = grouped.sort_values(by='buy_super', ascending=False).head(N)
                    # 後 N 大賣超
                    bottom_sell_super = grouped.sort_values(by='buy_super', ascending=True).head(N)
                    
                    selected_buy_traders = st.multiselect(
                        '選擇要查看的買超券商',
                        options=top_buy_super['securities_trader'],
                        default=top_buy_super['securities_trader'],
                        key='buy_traders'
                    )

                    selected_sell_traders = st.multiselect(
                        '選擇要查看的賣超券商',
                        options=bottom_sell_super['securities_trader'],
                        default=bottom_sell_super['securities_trader'],
                        key='sell_traders'
                    )

                    tab5.render_tab5(df_period, selected_buy_traders, selected_sell_traders)

                with tab6_:
                    tab6.render_tab6(df, N)

                with tab7_:
                    # 輸入要追蹤的券商名稱，允許多選
                    all_traders = grouped['securities_trader'].unique()
                    
                    if 'selected_track_traders' not in st.session_state or st.session_state.stock_code != stock_code:
                        st.session_state.selected_track_traders = []
                        
                    selected_track_traders = st.multiselect(
                        '選擇要追蹤的券商',
                        options=all_traders,
                        default=st.session_state.selected_track_traders,
                        key='track_traders'
                    )
                    st.session_state.selected_track_traders = selected_track_traders

                    tab7.render_tab7(df, selected_track_traders)
                    
                with tab8_:
                    tab8.render_tab8(df, N)
                with tab9_:
                    tab9.render_tab9(df, N)

