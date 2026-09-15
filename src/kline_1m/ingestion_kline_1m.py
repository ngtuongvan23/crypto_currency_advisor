import json
import websocket 
from confluent_kafka import Producer
# configure Kafka producer
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    #10.10.12.61:9092
}
producer = Producer(kafka_config)

def delivery_report(err, msg):
    if err is not None: 
        print(f" ❌ Failed to deliver: {err}")
    else:
        print(f" ✅ Delivered to {msg.topic()}: {msg.value().decode('utf-8')} : partition {msg.partition()} at offset {msg.offset()}")

# create API connection to Binance WebSocket for real-time data streaming
assets = ['btcusdt', 'ethusdt']
interval = '1m'
streamNames = ''
for asset in assets:
    streamNames += f'{asset}@kline_{interval}/'
socket = f'wss://stream.binance.com:9443/stream?streams={streamNames.rstrip("/")}'

# receieve and process real-time data from Binance WebSocket
# packaged data is sent to kafka 
def on_message(ws, message):
    candle = json.loads(message)
    rawPayload = candle['data']['k']
    is_closed = rawPayload['x']
    if is_closed == True:
    # Send data to Kafka
        data_to_kafka = {
            'symbol': rawPayload['s'],
            'start_time': rawPayload['t'],
            'open_price': float(rawPayload['o']),
            'high_price': float(rawPayload['h']),
            'low_price': float(rawPayload['l']),
            'close_price': float(rawPayload['c']),
            'volume': float((rawPayload['v']))
        }
        # convert data to json string and send to kafka topic
        value = json.dumps(data_to_kafka).encode('utf-8')
        producer.produce(topic='vannt_finance', value=value,callback=delivery_report)
        producer.poll(0)  # Trigger delivery report callbacks

def on_error(ws, error):
    print("❌" + str(error))

def on_close(ws, close_status_code, close_msg):
    producer.flush()  # Ensure all messages are sent before closing
    print("### closed ###")

def on_open(ws):
    print("Opened connection")

ws = websocket.WebSocketApp(socket,
                              on_open=on_open,
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close)

ws.run_forever()