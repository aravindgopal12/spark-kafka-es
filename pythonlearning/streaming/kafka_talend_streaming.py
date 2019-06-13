import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8-assembly_2.11:2.4.3,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 pyspark-shell'
from pyspark.sql.types import StructType

from pyspark.sql import SparkSession
from ast import literal_eval
spark = SparkSession \
    .builder \
    .appName("SSKafka") \
    .getOrCreate()

# default for startingOffsets is "latest", but "earliest" allows rewind for missed alerts
dsraw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "ec2-54-225-43-20.compute-1.amazonaws.com:9092") \
    .option("subscribe", "talend_topics") \
    .option("sep", "\t") \
    .option("startingOffsets", "earliest") \
    .load()



ds = dsraw.selectExpr("CAST(value AS STRING)")
print(type(dsraw))
print(type(ds))

rawQuery = dsraw \
        .writeStream \
        .queryName("qraw")\
        .format("console")\
        .start()

alertQuery = ds \
        .writeStream \
        .queryName("qalerts")\
        .format("console")\
        .start()

alertQuerys = ds \
        .writeStream \
    .outputMode("append") \
    .format("es") \
    .option("checkpointLocation", "D:/python_programs/pythonlearning/logfiles/logfile9") \
    .start("spark-test/doc-typess")



rawQuery.awaitTermination()
alertQuery.awaitTermination()
alertQuerys.awaitTermination()