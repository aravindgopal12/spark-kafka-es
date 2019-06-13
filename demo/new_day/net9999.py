from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import findspark
findspark.init('D:\Rcg\spark\spark-2.4.2-bin-hadoop2.7\spark-2.4.2-bin-hadoop2.7')
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)
lines = ssc.socketTextStream("192.168.15.215", 8888)
# print(type(lines))

words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()
ssc.start()             # Start the computation
ssc.awaitTermination()
