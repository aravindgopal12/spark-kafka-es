
from pyspark import SparkContext,SQLContext

import pprint

sc = SparkContext('local','pyspark-shell')  # if using locally

ut = sc.textFile("file:///D:/Rcg/Amazon_demo_data/es2.txt")
data = ut.map(lambda line: line.split("\t")) # assuming the file contains a header
print(type(data))
# print(data.count())
#
# print(data.take(5))
#
#
# mydata = data.collect()
#
#
# for x in mydata:
#     print(x)


print('lines in file: %s' % data.count())
# add up lenths of each line
chars = data.map(lambda s: len(s)).reduce(lambda a, b: a + b)
data.saveAsNewAPIHadoopFile(
    path='-',
    outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
    keyClass="org.apache.hadoop.io.NullWritable",
    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
    conf={ "es.resource" : "titanic/value_counts" })