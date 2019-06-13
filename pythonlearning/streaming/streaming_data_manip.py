from pyspark.sql.types import StructType
from ast import literal_eval
from pyspark.sql import SparkSession
import pandas as pd
import textblob
from textblob import Word, TextBlob
import nltk
#nltk.download('all')
from nltk.corpus import stopwords
import re

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

#spark.conf.set("spark.sql.execution.arrow.enabled", "true")

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

data = spark \
    .readStream \
    .option("sep", "\t") \
    .schema(userSchema) \
    .csv("D:/Rcg/Amazon_demo_data/data")



data.printSchema()
print(type(data))


#
query2 = data\
          .writeStream\
          .trigger(processingTime='10 seconds')\
          .format("console")\
          .start()



query2.awaitTermination()













#
# def clean_review(text):
#     '''
#     This fucntion iis to clean the daata
#     '''
#     letters_only = re.sub("[^a-zA-Z]", " ", str(text))  # Remove all digits and punctuation
#     words = str(letters_only).lower().split()  # Convert it into lower case
#     clean_words = [w for w in words if len(w) > 2]  # Remove 1/2 letter words
#     return (" ".join(clean_words))
#
#
# # Function to identify stop words
# def stop_words(review):
#     words = str(review).lower().split()  # split sentence into words
#     stops = set(stopwords.words("english"))  # import stop words from nltk
#     stop_words = [w for w in words if w in stops]  # identify stop words from the sentence
#     return (list(set(stop_words)))
#
#
# def no_stop_words(review):
#     words = str(review).lower().split()  # split sentence into words
#     stops = set(stopwords.words("english"))  # import stop words from nltk
#     stop_words = [w for w in words if w not in stops]  # identify stop words from the sentence
#     return (list(set(stop_words)))
#
#
# # Function to identify stop words
# def clean_stop_words(review):
#     words = str(review).lower().split()  # split sentence into words
#     stops = set(stopwords.words("english"))  # import stop words from nltk
#     stop_words = [w for w in words if w not in stops]  # identify stop words from the sentence
#     return (" ".join(stop_words))
#
#
# data.review_body['stop_removed'] = data.review_body['CleanedReview'].apply(lambda x: no_stop_words(x))
# data.review_body['clean_text'] = data.review_body['CleanedReview'].apply(lambda x: clean_stop_words(x))
# for x in data.collect():
#
#     analysis = TextBlob(clean_review(str(data.review_body['clean_text'][x])))
#
#     # print (analysis.sentiment)
#
#     analysis.sentiment.polarity
#
#     if analysis.sentiment.polarity < 0:
#         sentiment = "negative"
#         sentimentValue = -1
#     elif analysis.sentiment.polarity == 0:
#         sentiment = "neutral"
#         sentimentValue = 0
#     else:
#         sentiment = "positive"
#         sentimentValue = 1
#
#     print(sentiment + " " + str(data.review_body['clean_text'][x]))
# #

#
# data.awaitTermination()