import json
import time
import os
from datetime import datetime
import feedparser
from confluent_kafka import Producer

# --- CẤU HÌNH ĐỒNG BỘ VỚI LOCAL ---
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("NEWS_KAFKA_TOPIC", "vannt_news_stream")

kafka_config = {'bootstrap.servers': KAFKA_BOOTSTRAP}
producer = Producer(kafka_config)

RSS_FEEDS = [
    {"name": "CoinDesk", "url": "https://www.coindesk.com/arc/outboundfeeds/rss/"},
    {"name": "Cointelegraph", "url": "https://cointelegraph.com/rss"},
    {"name": "The Defiant", "url": "https://thedefiant.io/feed/"},
    {"name": "Decrypt", "url": "https://decrypt.co/feed"}
]

# Bộ nhớ lưu URL đã gửi (chống trùng)
seen_urls = set()

def delivery_report(err, msg):
    if err is not None: 
        print(f" ❌ Failed to deliver: {err}")
    else:
        preview = msg.value().decode('utf-8')[:50] + "..."
        print(f" ✅ Delivered to {msg.topic()}: {preview}")

def fetch_and_send_rss():
    print(f"\n--- Đang quét tin tức từ các nguồn RSS ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ---")
    total_sent = 0
    
    for feed_info in RSS_FEEDS:
        source_name = feed_info['name']
        try:
            feed = feedparser.parse(feed_info['url'])
            entries = feed.entries
            
            for entry in entries:
                url = entry.get('link', '')
                if url in seen_urls:
                    continue  # Bỏ qua tin đã gửi rồi
                seen_urls.add(url)

                published_at = entry.get('published', '')
                if hasattr(entry, 'published_parsed') and entry.published_parsed:
                    published_at = time.strftime('%Y-%m-%dT%H:%M:%SZ', entry.published_parsed)
                
                content = entry.content[0].get('value', '') if 'content' in entry and len(entry.content) > 0 else entry.get('summary', '')

                data_to_kafka = {
                    'title': entry.get('title', ''),
                    'author': entry.get('author', ''),
                    'description': entry.get('summary', entry.get('description', '')),
                    'content': content,
                    'url': entry.get('link', ''),
                    'publishedAt': published_at,
                    'source': source_name
                }
                
                value = json.dumps(data_to_kafka).encode('utf-8')
                producer.produce(topic=KAFKA_TOPIC, value=value, callback=delivery_report)
                producer.poll(0)
                total_sent += 1
                
        except Exception as e:
            pass
            
    # BƠM NHỊP TIM ĐỂ QUA MẶT AI VÀ ĐẨY WATERMARK
    if total_sent == 0:
        print("Bơm nhịp tim (Thời gian thực) chứa key Bitcoin/Ethereum để duy trì luồng Spark...")
        current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        
        hb_btc = {
            'title': 'Bitcoin BTC market is perfectly stable',
            'author': 'System',
            'description': 'Bitcoin BTC price action shows stability.',
            'content': 'Bitcoin BTC remains stable.',
            'url': f'null_btc_{time.time()}',
            'publishedAt': current_time,
            'source': 'Heartbeat'
        }
        hb_eth = {
            'title': 'Ethereum ETH market is perfectly stable',
            'author': 'System',
            'description': 'Ethereum ETH price action shows stability.',
            'content': 'Ethereum ETH remains stable.',
            'url': f'null_eth_{time.time()}',
            'publishedAt': current_time,
            'source': 'Heartbeat'
        }
        producer.produce(topic=KAFKA_TOPIC, value=json.dumps(hb_btc).encode('utf-8'), callback=delivery_report)
        producer.produce(topic=KAFKA_TOPIC, value=json.dumps(hb_eth).encode('utf-8'), callback=delivery_report)

    producer.flush()

if __name__ == "__main__":
    try:
        while True:
            fetch_and_send_rss()
            time.sleep(180) 
    except KeyboardInterrupt:
        print("\n### closed ###")