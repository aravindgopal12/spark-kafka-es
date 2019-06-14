
from pyspark import SparkContext,SQLContext

import pprint

sc = SparkContext('local','pyspark-shell')  # if using locally

ut = sc.textFile("file:///D:/Rcg/Amazon_demo_data/sentiment_51_records.txt")
data = ut.map(lambda line: line.split("\t")) # assuming the file contains a header
print(type(data))
print(data.count())

print(data.take(5))


mydata = data.collect()


for x in mydata:
    print(x)



