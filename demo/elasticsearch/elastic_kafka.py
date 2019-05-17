from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import os
import findspark
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host':'ec2-54-225-43-20.compute-1.amazonaws.com','port':9200}])

findspark.init('D:\Rcg\spark\spark-2.4.3-bin-hadoop2.7\spark-2.4.3-bin-hadoop2.7\spark-2.4.3-bin-hadoop2.7')

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8-assembly_2.11:2.4.3 pyspark-shell'

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)
kafkastream = KafkaUtils.createStream(ssc, 'ec2-54-225-43-20.compute-1.amazonaws.com:2181', 'mygroup', {'talend_topics': 1})
lines = kafkastream.map(lambda x: x[1])
# Print the first ten elements of each RDD generated in this DStream to the console
lines.pprint()


ssc.start()
for x in range(len(lines)):
 es.index(index="kafka_test",
            doc__type="kafka-test",
                  body={"marketplace": str(lines["marketplace"][x]),
                          "customer_id": str(lines["customer_id"][x]),
                          "review_id": str(lines["review_id"][x]),
                          "product_id": str(lines["product_id"][x]),
                          "product_parent": str(lines["product_parent"][x]),
                          "product_title": str(lines["product_title"][x]),
                          "product_sub_category": str(lines["product_sub_category"][x]),
                          "product_category": str(lines["product_category"][x]),
                          "star_rating": str(lines["star_rating"][x]),
                          "helpful_votes": str(lines["helpful_votes"][x]),
                          "total_votes": str(lines["total_votes"][x]),
                          "vine": str(lines["vine"][x]),
                          "verified_purchase": str(lines["verified_purchase"][x]),
                          "review_headline": str(lines["review_headline"][x]),
                          "review_body": str(lines["review_body"][x]),
                          "review_date": str(lines["review_date"][x])})
# Start the computation
ssc.awaitTermination()

