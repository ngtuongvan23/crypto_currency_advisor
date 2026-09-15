# 🚀 Real-time Crypto Trading Advisor Pipeline

## 1. Business Value

Thị trường Crypto biến động tính bằng giây. Hệ thống này được xây dựng để cung cấp **tín hiệu giao dịch theo thời gian thực (Real-time Trading Signals)** cho nhà đầu tư bằng cách tự động hóa hai luồng phân tích cốt lõi:

1. **Technical Analysis (TA):** Tính toán các chỉ báo kỹ thuật phức tạp (MACD, RSI, Bollinger Bands, OBV) trên dữ liệu nến 1 phút (Kline 1m) theo thời gian thực.
2. **Sentiment Analysis:** Phân tích tâm lý thị trường từ các trang tin tức (CoinDesk, Cointelegraph) để xác nhận xu hướng dòng tiền.

Hệ thống giúp loại bỏ cảm xúc con người khỏi quá trình ra quyết định, chỉ báo lệnh "HIGH CONFIDENCE LONG" khi cả kỹ thuật (K-line) và tin tức vĩ mô (News) cùng đồng thuận.

## 2. Architecture Diagram

<img width="1173" height="534" alt="image" src="https://github.com/user-attachments/assets/e1682320-b8c3-4343-a767-46519631dd6f" />
<img width="1280" height="720" alt="Slide2" src="https://github.com/user-attachments/assets/25f2444c-197a-4c72-8b2f-69d058809fc3" />

Link powpoint: https://thanglongedu-my.sharepoint.com/:p:/g/personal/a46958_thanglong_edu_vn/IQD0nzAT-FQPSJ6lFWqnHonJAcoDmhJnc4dth208FcKa4zY?e=jl1yD5

> 📍 **Project Scope & Team Collaboration**
> Dự án này là sản phẩm làm việc nhóm (6 thành viên). Repository này tập trung chủ yếu vào phần **Data Engineering & Streaming Pipeline** (đảm nhiệm bởi tôi).
> * **My Core Contribution (`/src/kline_pipeline`):** Thiết kế luồng Ingestion Real-time từ Binance WebSocket, giải quyết bài toán State Management trong PySpark Streaming, thiết kế Data Lake (HDFS) và lưu trữ tín hiệu (PostgreSQL).
> * **Teammate's Contribution (`/src/news_pipeline`):** Xây dựng luồng thu thập RSS feed và triển khai mô hình NLP (DistilFinBERT) thông qua Pandas UDFs trên Spark Workers để đánh giá Sentiment Score.
> 
> 

## 3. Engineering Challenges & Solutions (✨ Highlight)

Là người chịu trách nhiệm chính luồng dữ liệu thời gian thực `kline_1m`, tôi đã đối mặt và giải quyết các bài toán về Data Streaming:

### 🚨 Challenge 1: Bài toán "Cold Start" khi tính toán đường trung bình động (MA) trong Data Stream

* **Vấn đề:** Các chỉ báo như MA200 yêu cầu ít nhất 200 cây nến trong quá khứ để tính toán. Tuy nhiên, luồng Kafka stream chỉ chứa dữ liệu từ thời điểm bắt đầu chạy code (từ con số 0), khiến các chỉ báo bị sai lệch hoàn toàn trong 200 phút đầu tiên.
* **Giải pháp:** Tôi viết script `seed_data.py` gọi API REST của Binance để kéo 250 nến lịch sử gần nhất và lưu dưới dạng file Parquet (tối ưu hóa `use_deprecated_int96_timestamps=True` để tương thích hoàn hảo với Spark). Trong `final_adviors.py`, tôi cho Spark đọc khối dữ liệu lịch sử này từ HDFS và nối (Join) với luồng dữ liệu mới tới trước khi đẩy vào Window Functions.

### 🚨 Challenge 2: Giới hạn của PySpark Streaming với các hàm tích lũy (Cumulative Aggregation)

* **Vấn đề:** Tính toán chỉ báo OBV (On-Balance Volume) yêu cầu cộng dồn khối lượng giao dịch vô hạn từ quá khứ (`Window.unboundedPreceding`). PySpark Structured Streaming không hỗ trợ tác vụ này trực tiếp trên luồng dữ liệu không giới hạn vì rủi ro tràn RAM.
* **Giải pháp:**
1. Sử dụng cơ chế `foreachBatch` trong PySpark.
2. Tạo một bộ đệm (Global Buffer) bằng Pandas DataFrame (`global_klines`) giới hạn ở 100 nến gần nhất cho mỗi symbol.
3. Áp dụng logic tính toán OBV và MACD trên khối dữ liệu Pandas siêu nhẹ này, sau đó kết xuất kết quả cây nến **mới nhất** (row_number = 1) vào PostgreSQL. Điều này đảm bảo độ trễ siêu thấp (low latency) mà không làm sập bộ nhớ (OOM).



### 🚨 Challenge 3: Khớp nối 2 luồng dữ liệu lệch pha (Out-of-sync Data Streams)

* **Vấn đề:** Dữ liệu nến (`kline_1m`) đổ về đều đặn mỗi phút, nhưng dữ liệu tin tức (`news`) lại đến ngẫu nhiên, không theo chu kỳ.
* **Giải pháp:**
* Áp dụng **Watermarking** cho cả 2 luồng (1 phút cho Kline, 2 phút cho News).
* Thực hiện **Time-interval Join** để ghép nối điểm Sentiment của tin tức vào đúng cây nến trong khoảng thời gian `[kline.start_time - 2 minutes, kline.start_time]`.
* Đồng đội của tôi đã thiết kế thêm cơ chế "Heartbeat" (bơm dữ liệu rỗng) ở luồng News để thúc đẩy Watermark tiến lên ngay cả khi không có tin tức mới, giúp hệ thống không bị kẹt (stuck).



## 4. Tech Stack Rationale

* **Apache Kafka (KRaft mode):** Được chọn làm Message Broker để hứng dữ liệu tần số cao từ Binance WebSocket. Thay vì dùng Zookeeper cồng kềnh, tôi cấu hình Kafka KRaft mode trong Docker để tiết kiệm tài nguyên hệ thống (RAM/CPU), phù hợp với môi trường triển khai bị giới hạn tài nguyên.
* **Apache Spark (Structured Streaming):** Công cụ đáp ứng được nhu cầu xử lý các phép toán Window Functions phức tạp dựa trên thời gian thực (Time-based windowing) của dữ liệu chuỗi thời gian (Time-series data).
* **Hadoop HDFS & Parquet:** Dùng làm Data Lake lưu trữ Raw Data (Bronze Layer). Định dạng Parquet dạng cột (Columnar format) giúp Spark truy xuất 250 nến lịch sử cực kỳ nhanh để tính toán MA.
* **PostgreSQL:** Lưu trữ tín hiệu giao dịch cuối cùng (Gold Layer). Thiết kế bảng tĩnh chuẩn hóa giúp ứng dụng Web/Bot Telegram của End-user truy vấn nhanh chóng mà không cần động vào Data Lake.

## 5. Setup & Run Instructions

**Yêu cầu hệ thống:**

* Docker & Docker Compose
* Java 8/11 (Cho PySpark)
* Python 3.9+

**Bước 1: Khởi động Message Broker và Database**

```bash
# Khởi động Kafka (KRaft) qua Docker
docker-compose -f docker/docker-compose.yaml up -d

# Cài đặt thư viện và JDBC driver
bash setup_script.sh

```

**Bước 2: Chuẩn bị dữ liệu mồi (Giải quyết Cold Start)**

```bash
# Lấy lịch sử 250 nến đưa vào bộ nhớ đệm
python3 src/kline_pipeline/seed_data.py

```

**Bước 3: Khởi chạy luồng Ingestion**
*(Mở các terminal riêng biệt)*

```bash
# Hứng dữ liệu nến Binance
python3 src/kline_pipeline/ingestion_kline_1m.py

# Hứng dữ liệu RSS News (Teammate's flow)
python3 src/news_pipeline/ingestion_news.py

```

**Bước 4: Khởi chạy Spark Streaming Pipeline**

```bash
spark-submit src/main_advisor.py

```

---

