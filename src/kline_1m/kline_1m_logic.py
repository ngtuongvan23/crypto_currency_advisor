from pyspark.sql.functions import expr, from_json 
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from pyspark.sql.functions import lag,col,when,abs,round,avg
from pyspark.sql.window import Window

schema_kline = StructType([
        StructField("symbol", StringType(), True),
        StructField("start_time", LongType(), True),
        StructField("open_price", DoubleType(), True),
        StructField("high_price", DoubleType(), True),
        StructField("low_price", DoubleType(), True),
        StructField("close_price", DoubleType(), True),
        StructField("volume", DoubleType(), True)
    ]) 

def parse_kline_1m_stream(raw_kline_1m_df):
    #4.parse d  ata
    values_df = raw_kline_1m_df.withColumn("value",expr("cast(value as string)"))
    values_json = values_df.withColumn("parsed",from_json("value", schema_kline)).select("parsed")

    #5.flatten
    flatten_df = values_json.select("parsed.*")
    flatten_df = flatten_df.withColumn("start_time", expr("cast(start_time/1000 as timestamp)") )

    #6. watermark in 1m
    watermark_df = flatten_df.withWatermark("start_time", "1 minute")
    return watermark_df

#7. RSI
def caculate_RSI(batch_df):
    #RSI
    window_spec = Window.partitionBy("symbol").orderBy("start_time")    
    #Tinh gia chenh lech
    df_step1 = batch_df.withColumn("prev_close_price", lag("close_price",1).over(window_spec))
    df_step1 = df_step1.withColumn("change_price",round(col("close_price")-col("prev_close_price"),3))

    #Tach biet tang giam
    df_step2 = df_step1.withColumn("gain_close_price",\
                                   when(col("change_price")>=0,col("change_price")).otherwise(0))
    df_step2 = df_step2.withColumn("loss_close_price",\
                                   when(col("change_price")<0,abs("change_price")).otherwise(0))

    #Tính trung bình của cột Gain và cột Loss trong 14 phút gần nhất.
    window_14 = window_spec.rowsBetween(-13,Window.currentRow)
    df_step3 = df_step2.withColumn("avg_gain_14m",\
                                        round(avg("gain_close_price").over(window_14),3)
                                        )
    df_step3 = df_step3.withColumn("avg_loss_14m",\
                                        round(avg("loss_close_price").over(window_14),3))  

    # # Tinh RS 
    df_step4 = df_step3.withColumn("RSI",\
                                   when(col("avg_loss_14m") == 0,100)
                                    .otherwise(round(100-(100/(1+(col("avg_gain_14m") / col("avg_loss_14m")))),3)))                       
    RSI_df = df_step4.select("start_time","symbol", "RSI")
    return RSI_df

#MA
def caculate_MA(batch_df):
    #standard blueprint
    window_spec = Window.partitionBy("symbol").orderBy("start_time")

    #specific blueprint
    window_5m = window_spec.rowsBetween(-4, Window.currentRow)
    window_50m = window_spec.rowsBetween(-49, Window.currentRow)
    window_200m = window_spec.rowsBetween(-199, Window.currentRow)

    #caculate
    ma_df = batch_df.withColumn("MA5", round(avg("close_price").over(window_5m),3))\
            .withColumn("MA50", round(avg("close_price").over(window_50m),3))\
            .withColumn("MA200", round(avg("close_price").over(window_200m),3))
    ma_df = ma_df.select("start_time","symbol", "MA5","MA50","MA200")
    return ma_df

#Thao
from pyspark.sql.functions import expr, col, avg, when, stddev, lag, sum 
def caculate_general(batch_df):
    # cau hinh cua so thoi gian (blueprint)
    window_12 = Window.partitionBy("symbol").orderBy("start_time").rowsBetween(-11, 0)
    window_26 = Window.partitionBy("symbol").orderBy("start_time").rowsBetween(-25, 0)
    window_signal = Window.partitionBy("symbol").orderBy("start_time").rowsBetween(-8, 0)
    window_vol = Window.partitionBy("symbol").orderBy("start_time").rowsBetween(-20, -1)

    # Cấu hình cửa sổ cho Bollinger Bands (Mặc định tính trên chu kỳ SMA 20 phiên)
    window_bb = Window.partitionBy("symbol").orderBy("start_time").rowsBetween(-19, 0)

    # Cấu hình cửa sổ lũy kế vô hạn cho OBV (Quét từ dòng đầu tiên đến dòng hiện tại)
    window_obv = Window.partitionBy("symbol").orderBy("start_time").rowsBetween(Window.unboundedPreceding, 0)

    # Cấu hình cửa sổ lùi 1 dòng duy nhất để so sánh giá phiên hiện tại với phiên trước (Phục vụ OBV)
    window_lag1 = Window.partitionBy("symbol").orderBy("start_time")

    # TIẾN HÀNH TÍNH TOÁN CÁC CHỈ SỐ KỸ THUẬT
    # MACD
    df_indicators = batch_df \
        .withColumn("SMA_12", avg("close_price").over(window_12)) \
        .withColumn("SMA_26", avg("close_price").over(window_26))

    df_indicators = df_indicators.withColumn("MACD_Line", col("SMA_12") - col("SMA_26"))
    df_indicators = df_indicators.withColumn("Signal_Line", avg("MACD_Line").over(window_signal))
    df_indicators = df_indicators.withColumn("MACD_Histogram", round(col("MACD_Line") - col("Signal_Line"),3))

    # VOLUME SPIKE
    df_indicators = df_indicators.withColumn("Avg_Volume_Past", avg("volume").over(window_vol))

    df_indicators = df_indicators.withColumn(
        "Volume_Spike",
        when((col("volume") > (col("Avg_Volume_Past") * 1.5)) & (col("Avg_Volume_Past").isNotNull()), 1).otherwise(0)
    )

    # BOLLINGER BANDS (BB)
    df_indicators = df_indicators.withColumn("BB_Middle", avg("close_price").over(window_bb))
    df_indicators = df_indicators.withColumn("BB_StdDev", stddev("close_price").over(window_bb)) #Standard Deviation
    df_indicators = df_indicators.withColumn("BB_Upper", round(col("BB_Middle") + (col("BB_StdDev") * 2),3))
    df_indicators = df_indicators.withColumn("BB_Lower", round(col("BB_Middle") - (col("BB_StdDev") * 2),3))

    #ON-BALANCE VOLUME (OBV)
    df_indicators = df_indicators.withColumn("prev_close", lag("close_price", 1).over(window_lag1))
    df_indicators = df_indicators.withColumn("direction_volume",
        when(col("close_price") > col("prev_close"), col("volume"))
        .when(col("close_price") < col("prev_close"), -col("volume"))
        .otherwise(0.0)
    )
    df_final = df_indicators.withColumn("OBV", round(sum("direction_volume").over(window_obv),3))

    #KẾT QUẢ:
    df_final = df_final.select(
        "start_time",
        "symbol",
        "MACD_Histogram",
        "Volume_Spike",
        "BB_Upper",
        "BB_Lower",
        "OBV"
    )
    return df_final

# Chỉ được dùng bên trong foreachBatch
def driven_decision_kline(batch_df,spark_session):
    if batch_df.isEmpty():
        return

    # KHO LƯU TRỮ RAW DATA
    raw_storage_path = "hdfs://localhost:9000/user/hdoop/raw_kline_history"

    # Bước 1: Lưu cây nến hiện tại vào kho lịch sử
    batch_df.write.mode("append").parquet(raw_storage_path)

    # Bước 2: Kéo 250 dòng lịch sử gần nhất ra để tính toán
    try:
        # Lấy đủ 250 dòng để MA200 và EMA có đủ dữ liệu chạy
        history_df = spark_session.read.parquet(raw_storage_path) \
            .orderBy(col("start_time").desc()) \
            .limit(250)

        # Xếp lại theo chiều thời gian tăng dần cho Window quét
        history_df = history_df.orderBy(col("start_time").asc())
    except Exception:
        # Xử lý mẻ đầu tiên khi thư mục chưa được tạo
        history_df = batch_df

    # Bước 3: Ốp bộ công cụ Window vào dữ liệu lịch sử
    #blueprint
    window_spec = Window.partitionBy("symbol").orderBy("start_time")

    general_df = caculate_general(history_df)
    ma_df = caculate_MA(history_df)
    rsi_df = caculate_RSI(history_df)

    # Cấu trúc: df1.join(df2, "tên_cột_khóa\condition", "loại_join")
    caculated_df = history_df.join(ma_df,["start_time","symbol"], "inner")\
                            .join(general_df,["start_time","symbol"], "inner")\
                            .join(rsi_df,["start_time","symbol"], "inner")

    caculated_df = caculated_df.withColumn("OBV_prv", lag(col("OBV"),1).over(window_spec))

    # Bước 4: Ra quyết định
    #trend based on decided algorithm 
    advice_df = caculated_df.withColumn("Advice", 
        when((col("close_price") < col("MA50")) | (col("MA50") < col("MA200")),
            "❌ DROP ROW. Thị trường không có xu hướng tăng vĩ mô.")
        .when((col("close_price") > col("BB_Lower")) | (col("RSI") > 35),
            "⏳ KEEP OBSERVING. Giá chưa chiết khấu đủ sâu.Chờ transaction tiếp theo.")
        .when((col("MACD_Histogram") <= 0) | (col("close_price") < col("MA5")),
            "⏳ WAIT FOR TRIGGER. Lực mua chưa hồi phục. Tiếp tục chờ.")
        .when((col("Volume_Spike") == 0) & (col("OBV") <= col("OBV_prv")), 
            "⚠️ LOW CONFIDENCE LONG.\nTín hiệu kỹ thuật đẹp nhưng thiếu dòng tiền lớn bảo kê.\n-> Hành động: Bỏ qua lệnh HOẶC chỉ đi tiền 10% volume mục tiêu.")
        .otherwise("🚀 HIGH CONFIDENCE LONG.\nCá mập xác nhận đổ tiền đẩy giá (Khối lượng đột biến hoặc OBV phá đỉnh).")
        ).drop("OBV_prv")

    # Bước 5: Chỉ in ra Lời khuyên của cây nến MỚI NHẤT (Cắt bỏ lịch sử thừa)
    window_top1 = Window.partitionBy("symbol").orderBy(col("start_time").desc())
    final_output = advice_df.withColumn("rn", expr("row_number() over (partition by symbol order by start_time desc)")) \
                            .filter(col("rn") == 1) \
                            .drop("rn")

    return final_output


 