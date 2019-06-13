
import pandas as ps
from pyspark.shell import spark

read_data = ps.read_csv('D:/Rcg/Amazon_demo_data/amazon_reviews_us_Electronics_data.csv', sep='\t', header=0,names=['marketplace', 'review_date','customer_id'])

# print(read_data[['marketplace', 'review_date','customer_id']])

print(type(read_data))

