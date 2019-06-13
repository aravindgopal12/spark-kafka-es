from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
import os
from elasticsearch import Elasticsearch
# es = Elasticsearch([{'host':'localhost','port':9200}])



spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

userSchema = StructType().add("name", "string").add("age", "integer")

csvDF = spark \
    .readStream \
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("D:/Rcg/Amazon_demo_data/data2")

csvDF.printSchema()
print(type(csvDF))

query2 = csvDF\
          .writeStream\
          .trigger(processingTime='10 seconds')\
          .format("console")\
          .start()
pd = csvDF.toPandas()
pd.take(2)

# es_push = csvDF.writeStream \
#     .outputMode("append")\
#     .format("es")\
#     .option("checkpointLocation", "D:/python_programs/pythonlearning/logfiles/logfile2")\
#     .start("spark-test/doc-typess")


query2.awaitTermination()

