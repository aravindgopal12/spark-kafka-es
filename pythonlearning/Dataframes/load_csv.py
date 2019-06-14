
import pandas as ps
# from pyspark.shell import spark

read_data = ps.read_csv('D:/Rcg/Amazon_demo_data/data/sentiment_51_records.txt', sep='\t')

# print(read_data[['marketplace', 'review_date','customer_id']])
print(type(read_data))

print(read_data)

