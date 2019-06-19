# from operator import add, sub
# from time import sleep
#
# from demo.streaming_practice.streaming2 import evaluate
# from pyspark.context import xrange
#
# from pyspark import SparkContext,RDD
# from pyspark.streaming import StreamingContext
# from elasticsearch import Elasticsearch
# import findspark
# import streamz
# import json
#
# findspark.init('D:\Rcg\spark\spark-2.4.3-bin-hadoop2.7\spark-2.4.3-bin-hadoop2.7\spark-2.4.3-bin-hadoop2.7')
#
# #es = Elasticsearch(hosts='192.168.15.8:9202')
#
# sc = SparkContext(appName="PysparkNotebook")
# ssc = StreamingContext(sc, 1)
# # rdd = sc.parallelize([
# #     "2,Fitness", "3,Footwear", "4,Apparel"
# # ])
# es_read_conf = {"transactional":"true", 'es.index.auto.create':'true', "es.resource" : "spark_test/test-type" , "es.nodes" : "192.168.15.8:9202","es.index.read.missing.as.empty":"false"}
# #
# #
#
#
#
# # data = [(1, ""),(1, "a"),(2, "bcdf")]
# # sc.parallelize(data).saveAsNewAPIHadoopFile("path","org.apache.hadoop.mapreduce.lib.output.EsOutputFormat","org.apache.hadoop.io.IntWritable","org.apache.hadoop.io.Text",es_read_conf)
#
# # a = rdd.map(lambda x: tuple(x.split(",", 1))).saveAsNewAPIHadoopFile(
# #     path='-',
# #     outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
# #     keyClass="org.apache.hadoop.io.NullWritable",
# #     valueClass="org.ch.hadoop.mr.LinkedMapWritable",
# #     conf=es_read_conf
# # )
# # print(a)
# #
# inputData = [
#     [1,2,3],
#     [0],
#     [4,4,4],
#     [0,0,0,25],
#     [1,-1,10],
# ]
#
#
# rddQueue = []
# for datum in inputData:
#     rddQueue += [ssc.sparkContext.parallelize(datum)]
# inputStream = ssc.queueStream(rddQueue)
#
# print(type(inputStream))
#
# # json_input =  inputStream.map(lambda line: tuple.index(line[1],10,20))
# # sentiAnalysis = json_input.map(lambda line: (line,evaluate(line["text"])))
# # result2 = sentiAnalysis.map(lambda line: (None, line))
# #inputStream.saveAsTextFiles("D:/Rcg/spark/pycharm_output_test/kafka1")
# #inputStream.foreachRDD(lambda rdd: rdd.saveAsTextFile("D:/Rcg/spark/pycharm_output_test/kafka"))
# #
#
# # test = es.index(index="spark_test",
# #          doc_type="test-type",
# #          body={es.ingest})
# # #newAPIHadoopRDD(inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",keyClass="org.apache.hadoop.io.NullWritable", valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=es_read_conf)})
# #
# # # print(test)
# #
# # es_read_conf = {"transactional":"true", 'es.index.auto.create':'9202', "es.resource" : "my_indexss/dddoc" , "es.nodes" : "192.168.15.8:9202","es.index.read.missing.as.empty":"yes"}
# #
#
#
# #
#
#
#
#
# # es_rdd = sc.newAPIHadoopRDD(inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",keyClass="org.apache.hadoop.io.NullWritable", valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=es_read_conf)
# # #
# #a = inputStream.map(inputStream.take(2))
#
#
#
# # inputStream.transform(lambda rdd: rdd.map(lambda x :x[1]))
# # data.foreachRDD(lambda rdd :rdd.saveAsNewAPIHadoopFile(
# #     path='-',
# #     outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
# #     keyClass="org.apache.hadoop.io.NullWritable",
# #     valueClass="org.ch.hadoop.mr.LinkedMapWritable",
# #     conf=es_read_conf
# # ))
# inputStream.foreachRDD(lambda rdd: rdd.saveAsNewAPIHadoopFile(
#     path='-',
#     outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
#     keyClass="org.apache.hadoop.io.NullWritable",
#     valueClass="org.ch.hadoop.mr.LinkedMapWritable",
#     conf=es_read_conf
# ))
#
#
# print(type(inputStream))
# inputStream.pprint()
#
# ssc.start()
# sleep(5)
# ssc.stop(stopSparkContext=True, stopGraceFully=True)
#























from pyspark import SparkContext,RDD
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
import os
import findspark
from elasticsearch import Elasticsearch

import streamz
import json
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)
findspark.init('D:\Rcg\spark\spark-2.4.3-bin-hadoop2.7\spark-2.4.3-bin-hadoop2.7\spark-2.4.3-bin-hadoop2.7')
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8-assembly_2.11:2.4.3 pyspark-shell'
kafkastream = KafkaUtils.createStream(ssc, 'ec2-54-225-43-20.compute-1.amazonaws.com:2181', 'mygroup',{'talend_topics': 1})
lines = kafkastream.map(lambda x: x[1])

#lines.saveAsTextFiles("D:/Rcg/spark/pycharm_output_test/kafka_test")
# lines.foreachRDD(lambda rdd : rdd.saveAsTextFiles("D:/Rcg/spark/pycharm_output_test/kafka_test"))
lines.foreachRDD(lambda rdd: rdd.saveAsNewAPIHadoopFile(
    path='-',
    outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
    keyClass="org.apache.hadoop.io.NullWritable",
    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
   conf={
        "es.nodes": 'ec2-54-225-43-20.compute-1.amazonaws.com',
        "es.port": '9200',
        "es.resource": '%s/%s' % ('test', 'doc_type_name')
    }))

lines.pprint()
ssc.start()
ssc.awaitTermination()
