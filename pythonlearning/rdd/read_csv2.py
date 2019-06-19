# from pyspark.shell import spark
#
# sc = spark.sparkContext
# data = spark.read.format("csv").option("sep", "\t").option("header","false").load("D:/Rcg/Amazon_demo_data/sentiment_51_records.txt")
#
# print(type(data))
# print(data)
#
# # panda_df = data.toDF()
# # print(type(panda_df))
#
#
# #
# #
# # print(panda_df)
# # mydata = data.collect()
# #
# # print(data.columns)
# # print(data.select("REVIEW_BODY").show(5))
# #
# for x in data:
#     print(x)

from pyspark import SparkContext
from pyspark.sql import SQLContext
import pandas as pd
import pprint

sc = SparkContext('local','example')  # if using locally
data = [(1, ""),(1, "a"),(2, "bcdf")]
sc.parallelize(data).saveAsNewAPIHadoopFile("path","org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat","org.apache.hadoop.io.IntWritable","org.apache.hadoop.io.Text")
