import requests
import pandas as pd

def fetch_and_save_parquet(symbol):
    print(f"Đang kéo 250 nến liên tục của {symbol} từ Binance...")
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1m&limit=250"
    response = requests.get(url).json()
    
    df = pd.DataFrame(response, columns=[
        'start_time', 'open_price', 'high_price', 'low_price', 'close_price', 
        'volume', 'close_time', 'qav', 'num_trades', 'taker_base_vol', 'taker_quote_vol', 'ignore'
    ])
    
    df['symbol'] = symbol
    
    # SỬA LẠI Ở ĐÂY: Trả về kiểu thời gian (Datetime) thay vì kiểu số (Int)
    df['start_time'] = pd.to_datetime(df['start_time'], unit='ms') 
    
    df = df[['symbol', 'start_time', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']]
    df[['open_price', 'high_price', 'low_price', 'close_price', 'volume']] = df[['open_price', 'high_price', 'low_price', 'close_price', 'volume']].astype(float)
    
    filename = f"{symbol}_seed.parquet"
    
    # Thêm cấu hình use_deprecated_int96_timestamps=True để PyArrow ghi chuẩn 100% với định dạng Spark
    df.to_parquet(filename, index=False, engine='pyarrow', use_deprecated_int96_timestamps=True)
    print(f"✅ Đã lưu {filename}")

# Chạy mồi cho 2 mã
fetch_and_save_parquet('BTCUSDT')
fetch_and_save_parquet('ETHUSDT')