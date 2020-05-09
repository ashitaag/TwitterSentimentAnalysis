from kafka import KafkaConsumer
import json
# from pyspark import SparkContext,SparkConf
# from pyspark.streaming import StreamingContext
# from pyspark.streaming.kafka import KafkaUtils
from elasticsearch import Elasticsearch
from textblob import TextBlob
from csv import writer
import pandas as pd
es = Elasticsearch()
import os

def main():
    '''
    main function initiates a kafka consumer, initialize the tweetdata database.
    Consumer consumes tweets from producer extracts features, cleanses the tweet text,
    calculates sentiments and loads the data into postgres database
    '''
    # os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars spark-streaming-kafka-assembly_2.10-1.6.0.jar pyspark-shell'
    # conf = SparkConf().setAppName("Kafka-Spark")
    # sc = SparkContext(appName="KafkaSpark")
    # sc = SparkContext(conf=conf)
    # stream = StreamingContext(sc, 1)
    # map1 = {'twitter': 1}
    # kafkaStream = KafkaUtils.createStream(stream, 'localhost:2181', "name", map1)  # tried with localhost:2181 too
    #
    # print("kafkastream=", kafkaStream)
    # sc.stop()
    #et-up a Kafka consumer
    consumer = KafkaConsumer("twitter", group_id=None,bootstrap_servers=['localhost:9092'],enable_auto_commit=False,auto_offset_reset='latest')
    for msg in consumer:
        print(msg)
        dict_data = json.loads(msg.value)
        tweet = TextBlob(dict_data["text"])
        tweet = str(tweet)
        sentiment = TextBlob(tweet).sentiment
        polarity = sentiment.polarity
        print("polarity is", polarity)
        # add text and sentiment info to elasticsearch
        if polarity > 0:
            test_type = 'positive'
        elif polarity == 0:
            test_type = "neutral"
        else:
            test_type = "negative"
        es.index(index="tweet",
                 doc_type="test-type",
                 body={"author": dict_data["user"]["screen_name"],
                       "date": dict_data["created_at"],
                       "message": dict_data["text"],
                       "sentiment": test_type})
        print('\n')


if __name__ == "__main__":
    main()
