from pyspark.shell import spark

sc = spark.sparkContext
data = spark.read.format("csv").option("sep", "\t").option("header","false").load("D:/Rcg/Amazon_demo_data/sentiment_51_records.txt")

print(type(data))

# panda_df = data.toDF()
# print(type(panda_df))

print(data.select("marketplace").where("review_date = 6/12/2018"))
#
#
# print(panda_df)
# mydata = data.collect()
#

#
# for x in mydata:
#     print(x)
