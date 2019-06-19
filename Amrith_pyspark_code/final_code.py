from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext.getOrCreate()
ssc = StreamingContext(sc, 5)
from textblob import Word, TextBlob
from nltk.corpus import stopwords
import re
from elasticsearch import Elasticsearch
from pyspark.streaming.kafka import KafkaUtils
import os

ES_HOST = {"host": "54.225.43.20", "port": 9200}
es = Elasticsearch(hosts=[ES_HOST])
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8-assembly_2.11:2.4.3 pyspark-shell'
kafkastream = KafkaUtils.createStream(ssc, 'ec2-54-225-43-20.compute-1.amazonaws.com:2181', 'mygroup', {'talend_topics': 1})
new = kafkastream.map(lambda x: x[1])
# Print the first ten elements of each RDD generated in this DStream to the console

def amrit_func(new):
    def clean_review(text):
        # This fucntion is to clean the data#
        letters_only = re.sub("[^a-zA-Z]", " ", str(text))  # Remove all digits and punctuation
        words = str(letters_only).lower().split()  # Convert it into lower case
        clean_words = [w for w in words if len(w) > 2]  # Remove 1/2 letter words
        return (" ".join(clean_words))

    def clean_stop_words(review):
        words = str(review).lower().split()  # split sentence into words
        stops = set(stopwords.words("english"))  # import stop words from nltk
        stop_words = [w for w in words if w not in stops]  # identify stop words from the sentence
        return (" ".join(stop_words))

    def no_stop_words(review):
        words = str(review).lower().split()  # split sentence into words
        stops = set(stopwords.words("english"))  # import stop words from nltk
        stop_words = [w for w in words if w not in stops]  # identify stop words from the sentence
        return (list(set(stop_words)))

    for x in new.collect():
        analysis = TextBlob(clean_review(clean_stop_words(clean_review(x))))
        # print(analysis)
        # print (analysis.sentiment)

        analysis.sentiment.polarity

        if analysis.sentiment.polarity < 0:
            sentiment = "negative"
            sentimentValue = -1
        elif analysis.sentiment.polarity == 0:
            sentiment = "neutral"
            sentimentValue = 0
        else:
            sentiment = "positive"
            sentimentValue = 1

        print(sentiment + " " + str(clean_stop_words(clean_review(x))))
        es.index(index="kafka-realtime",
                 doc_type="test-type",
                 body={"Review": str("review_body"),
                       "sentimental_score": analysis.sentiment.polarity,
                       "subjectivity": analysis.sentiment.subjectivity,
                       "sentiment": sentiment,
                       "review_keywords": no_stop_words(x),
                       "sentimentValue": sentimentValue})


new.foreachRDD(amrit_func)

new.pprint()

ssc.start()
ssc.awaitTermination()


