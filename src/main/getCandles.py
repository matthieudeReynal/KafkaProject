import json
import time, requests
import urllib.request

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092")

while True:
    response=requests.get("https://api-public.sandbox.pro.coinbase.com/products/BTC-USD/candles?granularity=300")
    candles = json.loads(response.text)
    for candle in candles:
        print(candle)
        producer.send("candles2", json.dumps(candle).encode())
    print("{} Produced {} candle records".format(time.time(), len(candles)))
    time.sleep(15)