import streamlit as st
import pandas as pd
import plotly.graph_objs as go
import time
from datetime import datetime, timedelta
import os
import numpy as np
from streamlit_autorefresh import st_autorefresh  # 需要安裝 streamlit_autorefresh 套件

# 安裝 streamlit_autorefresh
# pip install streamlit-autorefresh

# 設定頁面標題
st.title("股票盤中模擬器")

# 側邊欄
st.sidebar.title("模擬設定")

# 初始化 session_state 中的股票代碼
if 'stock_code' not in st.session_state:
    st.session_state.stock_code = '1101'

# 輸入股票代碼
stock_code = st.sidebar.text_input('輸入股票代碼', st.session_state.stock_code)
st.session_state.stock_code = stock_code.strip()

# 檢查檔案是否存在
tick_file_path = f'tickDataSet/{stock_code}.csv'
stock_file_path = f'stockDataSet/{stock_code}.csv'

if not os.path.exists(tick_file_path):
    st.sidebar.error(f'檔案 {tick_file_path} 不存在，請確認股票代碼是否正確。')
    st.stop()

if not os.path.exists(stock_file_path):
    st.sidebar.error(f'檔案 {stock_file_path} 不存在，請確認股票代碼是否正確。')
    st.stop()

@st.cache_data
def load_stock_data(stock_id):
    df_stock = pd.read_csv(f"tickDataSet/{stock_id}.csv")
    df_stock['date'] = pd.to_datetime(df_stock['date'])
    return df_stock

df_stock = load_stock_data(stock_code)

# 確認必要欄位存在
required_columns_stock = {'date'}
if not required_columns_stock.issubset(df_stock.columns):
    st.sidebar.error(f'CSV 檔案缺少必要欄位：{required_columns_stock - set(df_stock.columns)}')
    st.stop()

# 獲取可用的模擬日期（排除最早一個日期，因為需要前一日的收盤價）
available_dates = df_stock['date'].dt.strftime('%Y-%m-%d').sort_values().unique()
if len(available_dates) < 2:
    st.sidebar.error("可用的模擬日期不足，至少需要兩天的數據。")
    st.stop()

# 初始化 session_state 中的日期範圍
if 'date_range' not in st.session_state or st.session_state.stock_code != stock_code:
    st.session_state.date_range = [available_dates[1], available_dates[-1]]  # 排除第一天

# 選擇模擬日期
selected_date = st.sidebar.selectbox("選擇模擬日期", options=available_dates[2:])

# 時間流速控制
time_speed = st.sidebar.slider("時間流速 (1x = 實時)", min_value=0.1, max_value=100.0, value=1.0)

# 加載數據
@st.cache_data
def load_data(stock_id, date):
    df = pd.read_csv(f"tickDataSet/{stock_id}.csv")
    df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['Time'])
    df = df[df['date'] == date]
    df = df.sort_values('datetime')
    df.reset_index(drop=True, inplace=True)
    return df

# 加載日線數據並獲取前一日的收盤價
@st.cache_data
def get_previous_close(stock_id, date):
    df_stock = pd.read_csv(f"stockDataSet/{stock_id}.csv")
    df_stock['date'] = pd.to_datetime(df_stock['date'])
    selected_date_dt = pd.to_datetime(date)
    prior_date = df_stock[df_stock['date'] < selected_date_dt]['date'].max()
    if pd.isna(prior_date):
        # 如果沒有前一日資料，返回 None
        return None
    prior_close = df_stock[df_stock['date'] == prior_date]['close'].values[0]
    return prior_close

df = load_data(stock_code, selected_date)
previous_close = get_previous_close(stock_code, selected_date)

if previous_close is None:
    st.error("無法找到前一日的收盤價，請選擇其他日期。")
    st.stop()

# 計算 y 軸上下限
upper_limit = previous_close * 1.1
lower_limit = previous_close * 0.9

# 設定模擬開始和結束時間
selected_date_datetime = pd.to_datetime(selected_date)
start_time = selected_date_datetime + pd.Timedelta(hours=9)
end_time = selected_date_datetime + pd.Timedelta(hours=13, minutes=30)

# 初始化前一次選擇的股票代碼和日期
if 'prev_stock_code' not in st.session_state:
    st.session_state.prev_stock_code = None

if 'prev_selected_date' not in st.session_state:
    st.session_state.prev_selected_date = None

# 檢測是否更改了股票代碼或選擇了不同的日期
stock_code_changed = st.session_state.prev_stock_code != stock_code
date_changed = st.session_state.prev_selected_date != selected_date

if stock_code_changed or date_changed:
    # 重置 session_state 中的模擬相關變量
    st.session_state.current_time = start_time
    st.session_state.paused = True
    st.session_state.orders = []      # 用戶訂單列表
    st.session_state.positions = []   # 持倉列表
    st.session_state.pnl_history = [] # 每日損益歷史
    st.session_state.times = []       # 重置時間列表
    st.session_state.prices = []      # 重置價格列表

    # 更新前一次選擇的股票代碼和日期
    st.session_state.prev_stock_code = stock_code
    st.session_state.prev_selected_date = selected_date

# 如果 'times' 和 'prices' 不在 session_state 中，初始化它們
if 'times' not in st.session_state:
    st.session_state.times = []
if 'prices' not in st.session_state:
    st.session_state.prices = []

# 開始/暫停按鈕
start_pause = st.sidebar.button("開始/暫停模擬")
if start_pause:
    st.session_state.paused = not st.session_state.paused

# 移動下單系統到側邊欄
st.sidebar.subheader("下單系統")
with st.sidebar.form("order_form"):
    order_type = st.selectbox("訂單類型", options=["市價", "限價"], index=1)
    buy_sell = st.selectbox("買/賣", options=["買進", "賣出"])
    quantity = st.number_input("數量", min_value=1, step=1)
    if order_type == "限價":
        price = st.number_input("價格", min_value=0.0, step=0.01)
    else:
        price = np.nan  # 使用 NaN 代替 "-"
    submitted = st.form_submit_button("下單")
    if submitted:
        order = {
            'type': order_type,
            'buy_sell': buy_sell,
            'quantity': quantity,
            'price': price,
            'filled_quantity': 0,
            'status': '開放',
            'order_time': st.session_state.current_time
        }
        st.session_state.orders.append(order)
        st.sidebar.success("訂單下單成功")

# 動態內容的佔位符
chart_placeholder = st.empty()
current_price_placeholder = st.empty()
orders_placeholder = st.empty()
positions_placeholder = st.empty()
pnl_placeholder = st.empty()

# 自動刷新設定
refresh_interval = 1000  # 每秒刷新一次
count = st_autorefresh(interval=refresh_interval, limit=9999999999, key="fizzbuzzcounter")

# 模擬主循環
if not st.session_state.paused and st.session_state.current_time <= end_time:
    current_time = st.session_state.current_time

    # 獲取當前時間的價格
    if current_time in df['datetime'].values:
        current_row = df[df['datetime'] == current_time].iloc[-1]
        current_price = current_row['deal_price']
    else:
        # 如果當前時間沒有交易，使用上一個價格
        current_price_series = df[df['datetime'] <= current_time]['deal_price']
        if not current_price_series.empty:
            current_price = current_price_series.iloc[-1]
        else:
            current_price = previous_close  # 設定為前一日收盤價
        current_row = pd.Series({'volume': 0})

    # 更新時間和價格列表
    st.session_state.times.append(current_time)
    st.session_state.prices.append(current_price)

    # 更新走勢圖
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=st.session_state.times, y=st.session_state.prices, mode='lines', name='價格'))
    fig.update_layout(
        title='走勢圖',
        xaxis=dict(range=[start_time, end_time], title='時間'),
        yaxis=dict(range=[lower_limit, upper_limit], title='價格'),
        margin=dict(l=40, r=40, t=40, b=40)
    )
    chart_placeholder.plotly_chart(fig, use_container_width=True)

    # 顯示當前成交報價
    current_price_placeholder.markdown(f"### 當前價格: {current_price} 在 {current_time.time()}")

    # 處理訂單
    trade_volume = current_row['volume'] if 'volume' in current_row else 0
    trade_price = current_price
    remaining_volume = trade_volume

    for order in st.session_state.orders:
        if order['status'] == '開放':
            # 判斷訂單是否可以成交
            can_fill = False
            if order['type'] == '市價':
                can_fill = True
            elif order['type'] == '限價':
                if order['buy_sell'] == '買進' and trade_price <= order['price']:
                    can_fill = True
                elif order['buy_sell'] == '賣出' and trade_price >= order['price']:
                    can_fill = True

            if can_fill:
                # 確定成交數量
                unfilled_quantity = order['quantity'] - order['filled_quantity']
                fill_quantity = unfilled_quantity  # 假設可以全部成交
                # 更新訂單
                order['filled_quantity'] += fill_quantity
                order['status'] = '已成交'
                # 記錄持倉
                position = {
                    'buy_sell': order['buy_sell'],
                    'quantity': fill_quantity,
                    'price': trade_price,
                    'datetime': current_time
                }
                st.session_state.positions.append(position

                )

    # 顯示活動訂單
    st.subheader("活動訂單")
    if st.session_state.orders:
        orders_data = []
        for i, order in enumerate(st.session_state.orders):
            orders_data.append({
                '訂單類型': order['type'],
                '買賣': order['buy_sell'],
                '數量': order['quantity'],
                '價格': order['price'],  # 已經是 float 或 NaN
                '狀態': order['status'],
                '已成交數量': order['filled_quantity'],
                '下單時間': order['order_time'].strftime("%H:%M:%S")
            })
        orders_df = pd.DataFrame(orders_data)
        # 確保 '價格' 欄位為 float
        orders_df['價格'] = pd.to_numeric(orders_df['價格'], errors='coerce')
        orders_placeholder.dataframe(orders_df)
    else:
        orders_placeholder.write("目前沒有活動訂單。")

    # 顯示持倉
    st.subheader("持倉")
    if st.session_state.positions:
        positions_data = []
        for position in st.session_state.positions:
            positions_data.append({
                '買賣': position['buy_sell'],
                '數量': position['quantity'],
                '價格': position['price'],
                '成交時間': position['datetime'].strftime("%H:%M:%S")
            })
        positions_df = pd.DataFrame(positions_data)
        # 確保 '價格' 欄位為 float
        positions_df['價格'] = pd.to_numeric(positions_df['價格'], errors='coerce')
        positions_placeholder.dataframe(positions_df)
    else:
        positions_placeholder.write("目前沒有持倉。")

    # 計算損益
    unrealized_pnl = 0.0
    for position in st.session_state.positions:
        if position['buy_sell'] == '買進':
            pnl = (current_price - position['price']) * position['quantity']
        else:
            pnl = (position['price'] - current_price) * position['quantity']
        unrealized_pnl += pnl

    pnl_placeholder.markdown(f"### 未實現損益: {unrealized_pnl:.2f}")

    # 更新當前時間
    st.session_state.current_time += timedelta(seconds=1 * time_speed)
    
    # 等待一段時間後自動刷新
    time.sleep(1 / time_speed)
    
    # 不需要手動 rerun，讓 st_autorefresh 來處理
else:
    # 在模擬暫停或結束時，顯示每日損益歷史
    st.subheader("每日損益歷史")
    if st.session_state.pnl_history:
        pnl_history_df = pd.DataFrame(st.session_state.pnl_history)
        st.dataframe(pnl_history_df)
    else:
        st.write("尚無損益記錄。")

    # 取消訂單功能
    st.subheader("取消訂單")
    open_orders = [i for i, order in enumerate(st.session_state.orders) if order['status'] == '開放']
    if open_orders:
        for i in open_orders:
            if st.button(f"取消訂單 {i}", key=f"cancel_{i}"):
                st.session_state.orders[i]['status'] = '已取消'
                st.success(f"訂單 {i} 已取消")
    else:
        st.write("目前沒有可取消的訂單。")
