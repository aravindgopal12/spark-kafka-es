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
sql_sc = SQLContext(sc)


ut = sc.textFile("D:/Rcg/Amazon_demo_data/amazon_reviews_us_Electronics_data.csv")
ut = ut.map(lambda line: line.split(','))
# print(type(ut))To know which type:Rdd or Dataframe or Streaming
# print(ut.count())//Count the data(Number of rows)
# print(ut.first())
for x in ut:
     print(x)      #Print the csv data exactly same as in the file(otherwise full data will be printed in a same line)
