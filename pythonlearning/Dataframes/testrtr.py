from pyspark import SparkContext
import findspark
findspark.init('D:\Rcg\spark\spark-2.4.3-bin-hadoop2.7\spark-2.4.3-bin-hadoop2.7\spark-2.4.3-bin-hadoop2.7')

sc = SparkContext("local[1]", "count app")
words = sc.parallelize (
   ["scala",
   "java",
   "hadoop",
   "spark",
   "akka",
   "spark vs hadoop",
   "pyspark",
   "pyspark and spark"]
)
counts = words.count()
print ("Number of elements in RDD -> %i" % (counts))