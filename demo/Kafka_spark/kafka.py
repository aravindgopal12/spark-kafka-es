from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import os
import findspark
from elasticsearch import Elasticsearch
es = Elasticsearch()
# findspark.init('D:\Rcg\spark\spark-2.4.3-bin-hadoop2.7\spark-2.4.3-bin-hadoop2.7\spark-2.4.3-bin-hadoop2.7')

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8-assembly_2.11:2.4.3 pyspark-shell'



# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)
kafkastream = KafkaUtils.createStream(ssc, 'ec2-54-225-43-20.compute-1.amazonaws.com:2181', 'mygroup', {'talend_topics': 1})
lines = kafkastream.map(lambda x: x[1])
# Print the first ten elements of each RDD generated in this DStream to the console
print(type(lines))
lines.pprint()



ssc.start()             # Start the computation
ssc.awaitTermination()

