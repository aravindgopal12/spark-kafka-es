from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import os
import findspark
from elasticsearch import Elasticsearch

es = Elasticsearch(hosts='ec2-54-225-43-20.compute-1.amazonaws.com:9200')

findspark.init('D:\Rcg\spark\spark-2.4.3-bin-hadoop2.7\spark-2.4.3-bin-hadoop2.7\spark-2.4.3-bin-hadoop2.7')

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8-assembly_2.11:2.4.3 pyspark-shell'

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)
kafkastream = KafkaUtils.createStream(ssc, 'ec2-54-225-43-20.compute-1.amazonaws.com:2181', 'mygroup', {'talend_topics': 1})
lines = kafkastream.map(lambda x: x[1])

# Print the first ten elements of each RDD generated in this DStream to the console
lines.pprint()
#lines.foreachRDD(lambda x: x[1])
es.index(index="spark_kafka_talendsss",
         doc_type="test-type",
         body={"marketplace": str(),
               "customer_id": str(),
               "review_id": str(),
               "product_id": str(),
               "product_parent": str(),
               "product_title": str(),
               "product_sub_category": str(),
               "product_category": str(),
               "star_rating": str(),
               "helpful_votes": str(),
               "total_votes": str(),
               "vine": str(),
               "verified_purchase": str(),
               "review_headline": str(),
               "review_body": str(),
               "review_date": str()})

ssc.start()
ssc.awaitTermination()

