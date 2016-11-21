# imports :
# -*- coding: utf-8 -*-
import time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from twitter import *
import dateutil.parser
import json
from pykafka import KafkaClient
# Connecting Streaming Twitter with Streaming Spark via Queue

# Conveniency class :

class Tweet(dict):
    def __init__(self, tweet_in):
        super(Tweet, self).__init__(self)
        if tweet_in and 'delete' not in tweet_in:
            self['timestamp'] = dateutil.parser.parse(tweet_in[u'created_at']).replace(tzinfo=None).isoformat()
            self['text'] = tweet_in['text'].encode('utf-8')
            #self['text'] = tweet_in['text']
            self['hashtags'] = [x['text'].encode('utf-8') for x in tweet_in['entities']['hashtags']]
            #self['hashtags'] = [x['text'] for x in tweet_in['entities']['hashtags']]
            self['geo'] = tweet_in['geo']['coordinates'] if tweet_in['geo'] else None
            self['id'] = tweet_in['id']
            self['screen_name'] = tweet_in['user']['screen_name'].encode('utf-8')
            #self['screen_name'] = tweet_in['user']['screen_name']
            self['user_id'] = tweet_in['user']['id']

# Twitter connection :            
def connect_twitter():
    access_token  =  ""
    token_secret  =  ""
    consumer_key  =  ""
    consumer_secret  =  ""
    auth = OAuth(access_token, token_secret, consumer_key, consumer_secret)
    stream = TwitterStream(auth = auth, secure = True)
    return stream


def takeAndPrint(time, rdd, num=1000):
    result = []
    taken = rdd.take(num + 1)
    url = 'http://localhost:9092/'

    print("-------------------------------------------")
    print("Time: %s" % time)
    print("-------------------------------------------")

    for record in taken[:num]:
        print(record)
        result.append(record)

    if len(taken) > num:
        print("...")
    print("")


# Main function :
if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingQueueStream")
    ssc = StreamingContext(sc, 1)
    # Instantiate the twitter_stream
    stream = connect_twitter()
    tweet_iter = stream.statuses.filter(track = "trump")
    for tweet in tweet_iter:
        if tweet.get('text'):
            data = tweet['text'].encode('utf8')
            print data
            client = KafkaClient(hosts='localhost:9092')
            topic = client.topics['test']

            with open('./file.txt', 'w') as f:
                json.dump(data, f)
            textFile = sc.textFile("./file.txt")

            # lines = textFile.map(lambda x: (x, str(x)))
            counts = textFile.flatMap(lambda line: line.split(" ")) \
                .map(lambda word: (word, 1)) \
                .reduceByKey(lambda a, b: a+b) \
                .map(lambda x:(x[1],x[0])) \
                .sortByKey(False) 
            string = counts.collect()
            text = ''.join(str(e) for e in string)
            with topic.get_sync_producer() as producer:
                producer.produce("Message from Spark:")
                producer.produce(text)
            time.sleep(1)
            counts.foreachRDD(takeAndPrint)

    ssc.start()
    ssc.stop(stopSparkContext=True, stopGraceFully=True)









