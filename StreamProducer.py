import tweepy
import kafka
from kafka import KafkaProducer
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from tweepy import Stream
#insert the credentials after creating a twitter developer account 
consumer_key = ''
consumer_secret = ''
access_token = ''
access_secret = ''

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)


class MyListener(StreamListener):
    def __init__(self):
        # localhost:9092 = Default Zookeeper Producer Host and Port Adresses
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    def on_data(self, data):
        #create a topic in kafka before running this file
        self.producer.send("twitter", data.encode('utf-8'))
        print(data)
        return True

    def on_error(self, status):
        print(status)
        return True


twitter_stream = Stream(auth, MyListener())
twitter_stream.filter(track=['#trump'])
