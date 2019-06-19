from pyspark.streaming import StreamingContext
from pyspark import SparkContext
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.elasticsearch:elasticsearch-hadoop:7.1.1 pyspark-shell'

sc = SparkContext(appName="PysparkNotebook")

textFile = sc.textFile('D:/Rcg/Amazon_demo_data/data/sentiment_51_records.txt')

print('lines in file: %s' % textFile.count())
# add up lenths of each line
wordCounts = textFile.flatMap(lambda line: line.split())
wordCounts = wordCounts.map(lambda word: (word, 1))
# add up word counts by key:word
wordCounts = wordCounts.reduceByKey(lambda a, b: a+b)
# sort in descending order by word counts
wordCounts = wordCounts.sortBy(lambda item: -item[1])
# collect the results in an array
results = wordCounts.collect()
# print the first ten elements
print(results[:10])

value_counts = textFile.map(lambda item: item[1]["marketplace"])
# value_counts = value_counts.map(lambda word: (word, 1))
# value_counts = value_counts.reduceByKey(lambda a, b: a+b)
# # put the results in the right format for the adapter
# value_counts = value_counts.map(lambda item: ('key', {
#     'field': 'marketplace',
#     'val': item[0],
#     'count': item[1]
# }))

print(value_counts)

# value_counts.saveAsNewAPIHadoopFile(
#     path='-',
#     outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
#     keyClass="org.apache.hadoop.io.NullWritable",
#     valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
#     conf={ "es.resource" : "titanic/value_counts" })
