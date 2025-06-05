import streamlit as st
import pandas as pd
import glob
import datetime
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

pd.set_option("styler.render.max_elements", 1000000)

#--------------------------------------------------
# 工具函式
#--------------------------------------------------
def read_csv_file(file, start_dt, end_dt, trader_name):
    """讀取單一 CSV，並依日期及券商篩選，並計算 net_buy = buy - sell。"""
    df = pd.read_csv(file)
    df["date"] = pd.to_datetime(df["date"])
    # 篩選日期與券商
    df = df[(df["date"] >= start_dt) & (df["date"] <= end_dt)]
    df = df[df["securities_trader"] == trader_name]
    if not df.empty:
        df["net_buy"] = df["buy"] - df["sell"]
    return df

def ensure_cache_dir_exists(cache_dir="app_broker/cache"):
    """若 cache_dir 不存在就先建立。"""
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

def load_existing_cache(cache_file):
    """
    從 CSV 載入快取的 pivot_df (index=日期, columns=股票名, values=net_buy)。
    回傳 (pivot_df, min_date, max_date)
    若檔案不存在或讀取失敗，回傳 (None, None, None)
    """
    if not os.path.exists(cache_file):
        return None, None, None
    try:
        pivot_df = pd.read_csv(cache_file, index_col=0, parse_dates=True)
        if pivot_df.empty:
            return None, None, None
        min_dt = pivot_df.index.min()
        max_dt = pivot_df.index.max()
        return pivot_df, min_dt, max_dt
    except Exception:
        return None, None, None

def save_cache(cache_file, pivot_df):
    """將 pivot_df 存成 CSV。"""
    pivot_df.to_csv(cache_file)

def process_missing_range(start_dt, end_dt, trader_name, stock_code_df):
    """
    處理 [start_dt, end_dt] 之間缺失的部分，
    從 CSV 讀取資料，並以進度條與剩餘時間訊息顯示處理進度，
    最後回傳 pivot_df（日期 x 股票名稱，值為 net_buy）。
    """
    all_csv_files = glob.glob("brokerDataSet/*.csv")
    if not all_csv_files:
        return pd.DataFrame()  # 回傳空的

    num_files = len(all_csv_files)
    df_list = []
    # 建立進度條與狀態訊息
    progress_bar = st.progress(0)
    status_text = st.empty()
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(read_csv_file, f, start_dt, end_dt, trader_name): f 
            for f in all_csv_files
        }
        completed = 0
        for future in as_completed(futures):
            try:
                result = future.result()
                if not result.empty:
                    df_list.append(result)
            except Exception as e:
                st.error(f"處理檔案 {futures[future]} 時發生錯誤: {e}")
            completed += 1
            progress_bar.progress(completed / num_files)
            elapsed_time = time.time() - start_time
            avg_time = elapsed_time / completed if completed > 0 else 0
            estimated_total = avg_time * num_files
            remaining_time = estimated_total - elapsed_time
            status_text.text(f"已處理 {completed}/{num_files} 檔案，剩餘約 {remaining_time:.1f} 秒")

    if not df_list:
        return pd.DataFrame()
    combined_df = pd.concat(df_list, ignore_index=True)
    if combined_df.empty:
        return pd.DataFrame()

    # 依日期與股票做加總 net_buy
    grouped_df = combined_df.groupby(["date", "stock_id"], as_index=False)["net_buy"].sum()

    # 檢查哪些股票代號無對應到股票資訊
    unknown_info = grouped_df[~grouped_df["stock_id"].isin(stock_code_df["StockID"])]
    if not unknown_info.empty:
        st.write("下列股票代號無法對應：")
        st.dataframe(unknown_info)

    # 使用 inner join 與股票代號對照表合併
    merged_df = pd.merge(
        grouped_df,
        stock_code_df,
        how="inner",
        left_on="stock_id",
        right_on="StockID"
    )
    # 轉換為 pivot 格式：index = date, columns = Stock Name, 值為 net_buy
    pivot_df_new = merged_df.pivot(index="date", columns="Stock Name", values="net_buy").fillna(0)
    return pivot_df_new

#--------------------------------------------------
# 主程式
#--------------------------------------------------
def main():
    st.title("券商交易查詢系統 (快取擴充版)")

    # 使用者輸入篩選的日期區間與券商
    start_date = st.date_input("開始日期", datetime.date(2023, 1, 3))
    end_date = st.date_input("結束日期", datetime.date(2023, 2, 1))
    trader_name = st.text_input("券商名稱", value="富邦北港")

    # 讀取股票代號對照表 (檔案須包含欄位：StockID,Stock Name)
    stock_code_df = pd.read_csv("taiwan_stock_codes.csv")

    # 確認 brokerDataSet 是否有資料
    all_csv_files = glob.glob("brokerDataSet/*.csv")
    if not all_csv_files:
        st.warning("找不到任何 brokerDataSet/*.csv 檔案，請先確認資料來源。")
        return

    # 確認快取資料夾存在
    ensure_cache_dir_exists("app_broker/cache")

    # 我們將快取檔案命名為： app_broker/cache/{trader_name}.csv
    cache_file = os.path.join("app_broker", "cache", f"{trader_name.replace(' ','_')}.csv")

    requested_start = pd.to_datetime(start_date)
    requested_end = pd.to_datetime(end_date)

    # 嘗試載入現有快取
    pivot_cache, cache_min_dt, cache_max_dt = load_existing_cache(cache_file)

    if pivot_cache is None:
        st.info("無現有快取，開始建立...")
        pivot_cache = process_missing_range(requested_start, requested_end, trader_name, stock_code_df)
        if pivot_cache.empty:
            st.warning("在指定區間內沒有任何交易資料，無法建立快取。")
            return
        save_cache(cache_file, pivot_cache)
        cache_min_dt = pivot_cache.index.min()
        cache_max_dt = pivot_cache.index.max()
        st.success(f"已建立快取檔案：{cache_file}")
    else:
        # 若現有快取已完全涵蓋使用者的區間，則不須更新
        if cache_min_dt <= requested_start and cache_max_dt >= requested_end:
            st.info(f"快取已涵蓋區間 {cache_min_dt.date()} ~ {cache_max_dt.date()}，直接使用。")
        else:
            st.info(f"現有快取涵蓋區間：{cache_min_dt.date()} ~ {cache_max_dt.date()}，開始更新缺少的部分...")
            new_min_dt = min(cache_min_dt, requested_start)
            new_max_dt = max(cache_max_dt, requested_end)
            missing_ranges = []
            if requested_start < cache_min_dt:
                missing_ranges.append((requested_start, cache_min_dt - pd.Timedelta(days=1)))
            if requested_end > cache_max_dt:
                missing_ranges.append((cache_max_dt + pd.Timedelta(days=1), requested_end))
            # 處理每一缺失區間，並合併到現有快取中
            for (m_start, m_end) in missing_ranges:
                st.write(f"處理缺少區間：{m_start.date()} ~ {m_end.date()}")
                pivot_new_part = process_missing_range(m_start, m_end, trader_name, stock_code_df)
                if not pivot_new_part.empty:
                    # 合併 pivot_cache 與新資料
                    all_index = pivot_cache.index.union(pivot_new_part.index)
                    all_columns = pivot_cache.columns.union(pivot_new_part.columns)
                    pivot_cache = pivot_cache.reindex(index=all_index, columns=all_columns, fill_value=0)
                    pivot_new_part = pivot_new_part.reindex(index=all_index, columns=all_columns, fill_value=0)
                    pivot_cache = pivot_cache + pivot_new_part
            # 更新快取檔
            cache_min_dt = new_min_dt
            cache_max_dt = new_max_dt
            save_cache(cache_file, pivot_cache)
            st.success(f"快取已更新，現涵蓋區間 {cache_min_dt.date()} ~ {cache_max_dt.date()}")

    # 最後根據使用者請求區間從 pivot_cache 中取出資料
    final_start = max(cache_min_dt, requested_start)
    final_end = min(cache_max_dt, requested_end)
    if final_start > final_end:
        st.warning(f"在 {start_date} ~ {end_date} 區間內無資料。")
        return

    final_df = pivot_cache.loc[final_start:final_end].copy()
    if final_df.empty:
        st.warning("沒有任何交易資料")
        return

    st.subheader(f"{trader_name} 在 {final_start.date()} ~ {final_end.date()} 每日買超 (net_buy) 數量")
    st.dataframe(final_df.style.format("{:,.0f}"))

    chart_type = st.selectbox("選擇圖表類型", ["Bar Chart", "Line Chart"])
    if chart_type == "Bar Chart":
        st.bar_chart(final_df)
    else:
        st.line_chart(final_df)

    # 計算累積持倉：不論查詢區間如何，累積持倉皆從最早有的資料開始累積
    cumulative_df = pivot_cache.cumsum()
    cumulative_query_df = cumulative_df.loc[final_start:final_end].copy()

    st.subheader(f"{trader_name} 在 {final_start.date()} ~ {final_end.date()} 累積持倉 (持有量)")
    st.dataframe(cumulative_query_df.style.format("{:,.0f}"))

    chart_type2 = st.selectbox("選擇累積持倉圖表類型", ["Bar Chart", "Line Chart"], key="cumulative_chart")
    if chart_type2 == "Bar Chart":
        st.bar_chart(cumulative_query_df)
    else:
        st.line_chart(cumulative_query_df)

if __name__ == "__main__":
    main()
