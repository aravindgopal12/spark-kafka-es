from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import os
import findspark
from elasticsearch import Elasticsearch
import pandas as pd
import textblob
from textblob import Word, TextBlob
import nltk
# nltk.download('all')
from nltk.corpus import stopwords
import re
import os
from wordcloud import WordCloud
# import matplotlib.pyplot as plt
from collections import Counter
es = Elasticsearch()
findspark.init('D:\Rcg\spark\spark-2.4.3-bin-hadoop2.7\spark-2.4.3-bin-hadoop2.7\spark-2.4.3-bin-hadoop2.7')

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8-assembly_2.11:2.4.3 pyspark-shell'



# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)
kafkastream = KafkaUtils.createStream(ssc, 'ec2-54-225-43-20.compute-1.amazonaws.com:2181', 'mygroup', {'talend_topics': 1})
lines = kafkastream.map(lambda x: x[1])
# Print the first ten elements of each RDD generated in this DStream to the console
print(type(lines))
lines.pprint()



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


lines.foreachRDD(

analysis = TextBlob(clean_review(clean_stop_words(clean_review(x))))

)