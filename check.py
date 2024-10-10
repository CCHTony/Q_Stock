import os
import pandas as pd

def count_stocks_below_threshold(directory, threshold=150):
    count = 0

    # 遍歷 stockDataSet 目錄
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            # 構建文件的完整路徑
            file_path = os.path.join(directory, filename)
            
            # 讀取 CSV 文件
            df = pd.read_csv(file_path)
            
            # 確保 DataFrame 不為空並存在 'close' 列
            if not df.empty and 'close' in df.columns:
                # 取得最後一列的收盤價
                latest_close_price = df['close'].iloc[-1]
                
                # 檢查收盤價是否低於 threshold
                if latest_close_price < threshold:
                    count += 1

    return count

# 設定資料夾路徑
directory = 'stockDataSet/'

# 計算最新收盤價低於150元的股票數量
result = count_stocks_below_threshold(directory, threshold=50)
print(f"最新收盤價低於150元的股票數量: {result}")
