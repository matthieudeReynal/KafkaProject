# KafkaProject



I tried here to handle Kafka, for this I used Coinbase API to get bitcoin candles every five minutes.

First run: sudo docker compose-up d

Then create needed topics: 
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic candles2,  
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic trendSample,  
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic smaSample, 

candles2 will contain raw data from API, we fill it by runnning: python3 getCandles.py (it's the producer)

Then run python3 consumer.py This consumer will create and fill a MongoDB base with raw data, you can test it with mongoRequests.py which create a request. The consumer also fill topics trendSample and smaSample. trendSample is composed by the date, close which are in the raw data, we add in the consumer the trend feature which is 1 if price goes up, -1 if it goes down. Trend is a basic but really useful feature for trend following trading stregies. Consumer also stocks time and close in smaSample, and add sma sample, which is the rolling mean of the 90 last closes. This indicator is useful for mean reversion trading strategies.

consumerTrend.py looks at three last trends and print trend behaviour if three last trends were equal. This helps user to have a better idea of trend.

consumerSma.py compare sma and close and warns the user if he should long or short bitcoins. Don't try this strategy in real! I made it really basic to better understand how Kafka works, not to become rich.

Web UI : http://localhost:3030
