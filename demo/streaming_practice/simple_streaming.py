from operator import add
from time import sleep

from pyspark import SparkContext,RDD
from pyspark.streaming import StreamingContext
from elasticsearch import Elasticsearch
import  os
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.elasticsearch:elasticsearch-spark-20_2.11:7.0.0-alpha2 pyspark-shell'
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars D:/Rcg/spark/spark-2.4.3-bin-hadoop2.7/spark-2.4.3-bin-hadoop2.7/spark-2.4.3-bin-hadoop2.7/jars/elasticsearch-spark-20_2.11-7.0.0-alpha2'
from pyspark.sql import SQLContext
sc = SparkContext(appName="PysparkNotebook")
ssc = StreamingContext(sc, 1)
# es = Elasticsearch(hosts='ec2-54-225-43-20.compute-1.amazonaws.com:9200')
es_read_conf = {"transactional":"true", 'es.index.auto.create':'true', "es.resource" : "spark_test/test-type", "es.nodes" : "54.225.43.20:9200","es.index.read.missing.as.empty":"false"}

inputData = [
    ["aravind","david"],
    [0],
    [4,4,4],
    [0,0,0,25],
    [1,-1,10],
]
rddQueue = []
for datum in inputData:
    rddQueue += [ssc.sparkContext.parallelize(datum)]
inputStream = ssc.queueStream(rddQueue)
print(type(inputStream))



# print(inputStream.foreachRDD(lambda rdd: rdd.collect()))
inputStream.pprint()
# inputStream.reduce(add).pprint()
# inputStream.count().pprint()

# rdd = sc.parallelize(inputStream)
# print(type(rdd))

# inputStream.foreachRDD(lambda rdd: sc.parallelize(inputStream).saveAsNewAPIHadoopFile(
#     path="-",
#     outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
#     keyClass="org.apache.hadoop.io.NullWritable",
#     valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
#     conf=es_read_conf
#     ))


# inputStream.reduce(add).pprint()
ssc.start()
sleep(5)
ssc.stop(stopSparkContext=True, stopGraceFully=True)



