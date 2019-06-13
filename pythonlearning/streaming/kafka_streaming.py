# from pyspark.sql.types import StructType
#
# from pyspark.sql import SparkSession
#
#
# spark = SparkSession \
#     .builder \
#     .appName("StructuredNetworkWordCount") \
#     .getOrCreate()
#
#
# df = spark \
#   .readStream \
#   .format("es") \
#   .option("key.serializer","org.apache.kafka.common.serialization.StringSerializer")\
#   .option("value.serializer","org.apache.kafka.common.serialization.StringSerializer")\
# .option("kafka.partition.assignment.strategy", "range")\
# .option("kafka.bootstrap.servers", "ec2-54-225-43-20.compute-1.amazonaws.com:2181") \
#   .option("subscribe", "talend_topics") \
#   .load()
#
# #df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
#
# es_push = df.writeStream \
#     .outputMode("append")\
#     .format("es")\
#     .option("checkpointLocation", "D:/python_programs/pythonlearning/Dataframes")\
#     .start("spark-testss/doc-typess")
#
#
#
# # query2 = df\
# #           .writeStream \
# #           .outputMode('append')\
# #           .trigger(processingTime='10 seconds')\
# #           .format("console")\
# #           .start()
#
#
# es_push.awaitTermination()
#
#



import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8-assembly_2.11:2.4.3,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 pyspark-shell'

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
    .option("startingOffsets", "earliest") \
    .load()



ds = dsraw.selectExpr("CAST(value AS STRING)")
print(type(dsraw))
print(type(ds))


rawQuery = dsraw \
        .writeStream \
        .queryName("qraw")\
        .format("memory")\
        .start()


alertQuery = ds \
        .writeStream \
        .queryName("qalerts")\
        .format("memory")\
        .start()


raw = spark.sql("select * from qraw")
raw.show()
raw.printSchema()
alerts = spark.sql("select * from qalerts")
alerts.show()


rawQuery.awaitTermination()
alertQuery.awaitTermination()

# rddAlertsRdd = alerts.rdd.map(lambda alert: literal_eval(alert['value']))
# rddAlerts = rddAlertsRdd.collect()
# type(rddAlerts)
# print(rddAlerts[1])
