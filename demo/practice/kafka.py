import os
from pyspark import SparkConf, SparkContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import TopicAndPartition
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.2 pyspark-shell'

sc = SparkContext(master="local",appName="Spark_kafka_Demo")
ssc = StreamingContext(sc, 5)
topic = "talend_topic"
brokers = "ec2-54-225-43-20.compute-1.amazonaws.com:2181"
partition = 0
start = 0
#topicpartion = TopicAndPartition(topic, partition)
#fromoffset = {topicpartion: int(start)}
directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
#kafkaStream = KafkaUtils.createStream(ssc, 'ec2-54-225-43-20.compute-1.amazonaws.com:2181', 'mygroup', {'talend_topic': 1})
lines = directKafkaStream.map(lambda x: x[1])
lines.pprint()

#ssc.start()
#ssc.awaitTermination()