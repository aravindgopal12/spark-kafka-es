from pyspark.sql.types import StructType

from pyspark.sql import SparkSession
import  pandas as pd

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

userSchema = StructType()\
    .add("marketplace", "string")\
    .add("customer_id", "string")\
    .add("REVIEW_ID" , "string")\
    .add("PRODUCT_ID", "string",)\
    .add("PRODUCT_PARENT", "string")\
    .add("PRODUCT_TITLE", "string")\
    .add("PRODUCT_SUB_CATEGORY", "string")\
    .add("PRODUCT_CATEGORY" , "string")\
    .add("STAR_RATING", "string",)\
    .add("HELPFUL_VOTES", "string")\
    .add("TOTAL_VOTES", "string")\
    .add("VINE" , "string")\
    .add("VERIFIED_PURCHASE", "string",)\
    .add("REVIEW_HEADLINE", "string")\
    .add("REVIEW_BODY", "string")\
    .add("REVIEW_DATE", "string")

csvDF = spark \
    .readStream \
    .option("sep", "\t") \
    .schema(userSchema) \
    .csv("D:/Rcg/Amazon_demo_data/data")

csvDF.printSchema()
print(type(csvDF))



query2 = csvDF\
          .writeStream\
          .trigger(processingTime='10 seconds')\
          .format("console")\
          .start()


es_push = csvDF.writeStream \
    .outputMode("append")\
    .format("es")\
    .option("checkpointLocation", "D:/python_programs/pythonlearning/logfiles/logfile6")\
    .start("streaming-twitter/doc-typess")


query2.awaitTermination()
es_push.awaitTermination()

