import json
from kafka import KafkaConsumer
from kafka import KafkaProducer
import time
import datetime

consumer = KafkaConsumer("trendSample",
     bootstrap_servers='localhost:9092',
     group_id="candles-analysis",
     value_deserializer=lambda x: json.loads(x.decode('utf-8'))
     )

producer = KafkaProducer(bootstrap_servers="localhost:9092")

i = 0
messages = []
for message in consumer:
    message = message.value
    messages.append(message)
    # convert epoch time to iso format for better understanding
    date = datetime.datetime.fromtimestamp(message[0]).strftime('%c')
    if i >= 2:
        # if trend is stable over 3 candles we inform the user
        if(message[2]==messages[i-1][2] and message[2]==messages[i-2][2]):
            if message[2] == 1:
                print(date, " : growing trend")
            if message[2] == -1:
                print(date, " : decreasing trend")
    i = i+1