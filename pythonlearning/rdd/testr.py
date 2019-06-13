# from pyspark import SparkContext
import findspark
findspark.init('D:\Rcg\spark\spark-2.4.3-bin-hadoop2.7\spark-2.4.3-bin-hadoop2.7\spark-2.4.3-bin-hadoop2.7')
#
# sc = SparkContext("local[1]", "count app")
# words = sc.parallelize(
#    ["scala",
#    "java",
#    "hadoop",
#    "spark",
#    "akka",
#    "spark vs hadoop",
#    "pyspark",
#    "pyspark and spark"]
# )
# counts = words.count()
# a = words.collect()
# print(a)
# print ("Number of elements in RDD -> %i" % (counts))
# es_conf = {"es.nodes": "192.168.15.8", "es.port": "9202", "es.nodes.client.only": "true",
#            "es.resource": "sensor_counts/metrics"}
# b = sc.parallelize(a)
# out = b.saveAsNewAPIHadoopFile(path='-',
#      outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
#      keyClass="org.apache.hadoop.io.NullWritable",
#      valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
#      conf=es_conf)
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
if __name__ == "__main__":
    conf = SparkConf().setAppName("WriteToES")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    es_conf = {"es.nodes" : "ec2-54-225-43-20.compute-1.amazonaws.com","es.port" : "9200","es.nodes.client.only" : "true","es.resource" : "sensor_counts/metrics"}
    es_df_p = sqlContext.read.text("D:/Rcg/Amazon_demo_data/sentiment_51_records.txt")
  #  es_df_pf= es_df_p.groupBy("element_id").count().map(lambda (a,b): ('id',{'element_id': a,'count': b}))
    push = sc.parallelize(es_df_p)
    push.saveAsNewAPIHadoopFile(
     path='-',
     outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
     keyClass="org.apache.hadoop.io.NullWritable",
     valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
     conf=es_conf)