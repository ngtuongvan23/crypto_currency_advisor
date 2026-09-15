import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, when
from kline_1m.kline_1m_logic import driven_decision_kline, parse_kline_1m_stream
from news.news_logic import process_news_sentiment

# Khởi tạo bộ đệm gom nến toàn cục
global_klines = pd.DataFrame()

# 1. Tao doi tuong spark 
spark = SparkSession.builder.appName("Crypto_Advisor")\
    .master("local[*]") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true")\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.postgresql:postgresql:42.7.3")\
    .config("spark.sql.execution.arrow.pyspark.enabled", "false")\
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")\
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")\
    .config("spark.sql.shuffle.partitions", "2")\
    .config("spark.default.parallelism", "2")\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Tao doi tuong kafka (Đổi sang Local)
raw_kline = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers","localhost:9092")\
    .option("subscribe","vannt_finance")\
    .option("startingOffsets","latest")\
    .load()

raw_news = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers","localhost:9092")\
    .option("subscribe","vannt_news_stream")\
    .option("startingOffsets","latest")\
    .load()

# 3. Parse data, watermark
kline_stream = parse_kline_1m_stream(raw_kline).alias("kline")

# THÊM LẠI DÒNG WATERMARK 2 min VÀO ĐÂY
news_stream = process_news_sentiment(raw_news) \
    .withWatermark("publishedAt", "2 minute") \
    .alias("news")

#4. Store silver data
# Luồng ghi Nến xuống HDFS
kline_datalake = kline_stream.writeStream\
    .format("parquet")\
    .option("path", "hdfs://localhost:9000/user/hdoop/crypto_data_lake/kline_1m/")\
    .option("checkpointLocation", "hdfs://localhost:9000/user/hdoop/checkpoints/kline/")\
    .partitionBy("symbol")\
    .trigger(processingTime="15 minutes")\
    .start()

# Luồng ghi Tin tức xuống HDFS
news_datalake = news_stream.writeStream\
    .format("parquet")\
    .option("path", "hdfs://localhost:9000/user/hdoop/crypto_data_lake/news/")\
    .option("checkpointLocation", "hdfs://localhost:9000/user/hdoop/checkpoints/news/")\
    .partitionBy("symbol")\
    .trigger(processingTime="15 minutes")\
    .start()

# 4. Join 2 luồng dữ liệu VÀ CHỌN LỌC CỘT NGAY LẬP TỨC
joined_stream = kline_stream.join(
    news_stream,
    expr("""
        kline.symbol = news.symbol AND
        news.publishedAt >= kline.start_time - interval 2 minute AND
        news.publishedAt <= kline.start_time
    """),
    "leftOuter"
).select(
    # Bóc tách rõ ràng các cột cần thiết, bỏ hết rác thừa
    col("kline.start_time"), col("kline.symbol"), col("kline.open_price"),
    col("kline.high_price"), col("kline.low_price"), col("kline.close_price"),
    col("kline.volume"), col("news.sentiment_score")
)

# 5. Xử lý mỗi batch với TUYỆT KỸ BUFFER
def process_final_batch(batch_df, batch_id):
    global global_klines
    if batch_df.isEmpty(): return

    # KHÔNG CẦN DÙNG ALIAS Ở ĐÂY NỮA VÌ ĐÃ ĐƯỢC LỌC SẠCH Ở BƯỚC JOIN
    agg_batch_df = batch_df.groupBy(
        "start_time", "symbol", "open_price", "high_price", "low_price", "close_price", "volume"
    ).agg(expr("avg(sentiment_score)").alias("avg_sentiment")).fillna({"avg_sentiment": 0.0})

    pure_kline_1m = agg_batch_df.select("start_time", "symbol", "open_price", "high_price", "low_price", "close_price", "volume")

    # Ép kiểu sang Pandas và gom vào bộ nhớ
    current_pdf = pure_kline_1m.toPandas()
    if current_pdf.empty: return

    global_klines = pd.concat([global_klines, current_pdf])
    global_klines = global_klines.drop_duplicates(subset=['start_time', 'symbol']).sort_values('start_time')
    global_klines = global_klines.groupby('symbol').tail(100).reset_index(drop=True) 

    # Bơm 100 nến vào hàm tính Toán Kỹ thuật
    buffered_spark_df = spark.createDataFrame(global_klines)
    tech_df = driven_decision_kline(buffered_spark_df, spark)

    if tech_df is None or tech_df.isEmpty():
        print(f"[{batch_id}] ⏳ Đã gom {len(global_klines)} nến, chờ đủ 26 nến để tính MACD...")
        return

    # Lọc chỉ ghi vào DB những nến VỪA MỚI đóng ở Batch này
    current_times = current_pdf['start_time'].tolist()
    tech_df_new = tech_df.filter(col("start_time").isin(current_times))

    if tech_df_new.isEmpty(): return

    final_df = tech_df_new.join(agg_batch_df.select("start_time", "symbol", "avg_sentiment"), ["start_time", "symbol"], "inner")

    final_advice_df = final_df.withColumn("Final_Advice",
        when((col("Advice").like("%HIGH CONFIDENCE LONG%")) & (col("avg_sentiment") >= 0.5),
             expr("concat(Advice, '\n🌟 TIN TỨC BƠM THÊM: Thị trường đang fomo mạnh, vào ngay!')"))
        .when((col("Advice").like("%HIGH CONFIDENCE LONG%")) & (col("avg_sentiment") <= -0.5),
             expr("concat(Advice, '\n🚨 CẢNH BÁO: Kỹ thuật báo mua nhưng tin xấu bủa vây, cẩn thận sập bẫy!')"))
        .otherwise(col("Advice"))
    )

    print(f"\n[{batch_id}] --- TỔNG HỢP KLINE & NEWS ADVISOR ---")
    final_advice_df.select("start_time", "symbol", "close_price", "Final_Advice").show(truncate=False)
    # 6. Ghi vào Local PostgreSQL
    jdbc_url = "jdbc:postgresql://localhost:5432/crypto-db-instance"
    db_properties = {"user": "postgres", "password": "123456", "driver": "org.postgresql.Driver"}

    try:
        # TẠO BỘ LỌC ÉP KHUÔN ĐÚNG 15 CỘT CỦA DATABASE TRƯỚC KHI GHI
        db_write_df = final_advice_df.select(
            "start_time", "symbol", "close_price", "MACD_Histogram", 
            "Volume_Spike", "BB_Upper", "BB_Lower", "OBV", "RSI", 
            "MA200", "MA50", "MA5", "Advice", "avg_sentiment", "Final_Advice"
        )

        # Ghi vào Database
        db_write_df.write.mode("append").jdbc(url=jdbc_url, table="advisor_signals", properties=db_properties)
        print(f"[{batch_id}] ✅ Đã xả tín hiệu thành công vào PostgreSQL Local!")
    except Exception as e:
        print(f"[{batch_id}] ❌ LỖI GHI DB: {str(e)}")

# 7. Start stream (Lưu Checkpoint ra thư mục nội bộ)
query = joined_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(process_final_batch) \
    .option("checkpointLocation", "./checkpoints/final_advisor") \
    .start()
 
spark.streams.awaitAnyTermination()
