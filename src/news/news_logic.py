from pyspark.sql.functions import from_json, col, lower, concat, lit, when
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import pandas as pd
from typing import Iterator
from pyspark.sql.functions import pandas_udf

# 1. Định nghĩa cấu trúc dữ liệu JSON
news_schema = StructType([
    StructField("title", StringType(), True),
    StructField("author", StringType(), True),
    StructField("description", StringType(), True),
    StructField("content", StringType(), True),
    StructField("url", StringType(), True),
    StructField("publishedAt", StringType(), True),
    StructField("source", StringType(), True),
    StructField("sentiment_score", FloatType(), True)
])

# 1. Khai báo một biến toàn cục nằm ngoài hàm đóng vai trò làm bộ nhớ đệm (Cache)
_SENTIMENT_PIPE = None

# 2. SENTIMENT ANALYSIS BẰNG DISTILFINBERT (Pandas UDF)
@pandas_udf(FloatType())
def sentiment_udf(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    """
    Tính sentiment score bằng DistilFinBERT xử lý theo Batch trên mỗi Worker.
    Tối ưu hóa bằng cơ chế Lazy Loading để tránh reload mô hình liên tục.
    """
    # Gọi biến toàn cục vào bên trong hàm
    global _SENTIMENT_PIPE
    
    # Kỹ thuật Lazy Loading: Chỉ tải MỘT LẦN DUY NHẤT khi Worker mới tinh được dựng lên
    if _SENTIMENT_PIPE is None:
        import torch
        from transformers import pipeline

        print("[DistilFinBERT] 🚀 Đang khởi tạo mô hình sentiment analysis trên Worker (Chỉ chạy một lần)...")
        _SENTIMENT_PIPE = pipeline(
            "sentiment-analysis",
            model="mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis",
            tokenizer="mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis"
        )
        torch.set_num_threads(1) 
        print("[DistilFinBERT] ✨ Mô hình đã sẵn sàng và được lưu vào bộ nhớ Worker!")

    # Duyệt qua các batch dữ liệu
    for s in iterator:
        texts = s.fillna("").astype(str).str.slice(0, 512).tolist()
        
        try:
            # Dùng thẳng biến toàn cục đã được tối ưu
            results = _SENTIMENT_PIPE(texts)
        except Exception as e:
            print(f"[DistilFinBERT ERROR] {str(e)}")
            yield pd.Series([0.0] * len(texts))
            continue
        
        scores = []
        for res, text in zip(results, texts):
            if not text or len(text.strip()) < 5:
                scores.append(0.0)
                continue
                
            label = res['label'].lower()
            confidence = res['score']
            
            if label == "positive":
                score = round(confidence, 3)
            elif label == "negative":
                score = round(-confidence, 3)
            else: # neutral
                score = 0.0
                
            scores.append(float(score))
            
        yield pd.Series(scores)
# ═══════════════════════════════════════════════════════════════════════
# 3. HÀM GỌI LOGIC CHÍNH (Dùng để import vào file Final)
# ═══════════════════════════════════════════════════════════════════════
def process_news_sentiment(raw_news_df):
    """
    Hàm nhận DataFrame thô từ Kafka, thực hiện parse JSON, lọc từ khóa,
    tạo cột symbol và chấm điểm Sentiment.
    """
    # 3.1. Giải mã dữ liệu binary từ Kafka
    news_df = raw_news_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), news_schema).alias("data")) \
        .select("data.*")

    # 3.2. Lọc tin tức liên quan đến Crypto
    btc_eth_news = news_df.filter(
        (lower(col("title")).rlike("bitcoin|btc|ethereum|eth|crypto|cryptocurrency")) |
        (lower(col("description")).rlike("bitcoin|btc|ethereum|eth|crypto|cryptocurrency")) |
        (lower(col("content")).rlike("bitcoin|btc|ethereum|eth|crypto|cryptocurrency"))
    )
    
    # 3.3. TẠO CỘT KHÓA 'symbol' ĐỂ CHUẨN BỊ JOIN VỚI BẢNG NẾN
    # Map từ khóa trong bài báo sang chuẩn mã giao dịch của sàn Binance
    btc_eth_news = btc_eth_news.withColumn(
        "symbol",
        when(lower(col("title")).rlike("bitcoin|btc") | lower(col("description")).rlike("bitcoin|btc"), "BTCUSDT")
        .when(lower(col("title")).rlike("ethereum|eth") | lower(col("description")).rlike("ethereum|eth"), "ETHUSDT")
        .otherwise("UNKNOWN")
    ).filter(col("symbol") != "UNKNOWN") # Bỏ đi những tin chung chung không rõ đồng nào

    # 3.4. Ghép nội dung và chấm điểm Sentiment
    processed_news_df = btc_eth_news.withColumn(
        "combined_text",
        concat(col("title"), lit(" "), col("description"))
    ).withColumn(
        "sentiment_score",
        sentiment_udf(col("combined_text"))
    )
    
    # 3.5. Ép kiểu thời gian để đồng bộ chuẩn Watermark với file Kline
    processed_news_df = processed_news_df.withColumn("publishedAt", col("publishedAt").cast("timestamp"))

    # Trả về bảng dữ liệu đã hoàn thiện, sẵn sàng để gộp
    return processed_news_df