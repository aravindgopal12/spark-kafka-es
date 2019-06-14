# Import statements
import pandas as pd
import textblob
from textblob import Word, TextBlob
import nltk
#nltk.download('all')
from nltk.corpus import stopwords
import re
import os
from wordcloud import WordCloud
# import matplotlib.pyplot as plt
from collections import Counter

from elasticsearch import Elasticsearch

# create instance of elasticsearch
es = Elasticsearch(hosts='ec2-54-225-43-20.compute-1.amazonaws.com:9200')

dataobj = pd.read_excel("D:/Rcg/Amazon_demo_data/Sentimental_Analysis_Data_Revised1.xlsx", encoding='utf8')

# dataFileUrl = r"/var/python_scripts/twittersent/Sentimental_Analysis_Data_Revised.xlsx"
# dataobj= pd.read_table(dataFileUrl)

print(dataobj)

print(type(dataobj))


dataobj.columns.str.match('Unnamed')
print(dataobj.columns.str.match('review_body'))


#
# data = dataobj.loc[:, ~dataobj.columns.str.contains('^Unnamed')]
#
#
# # data.columns
#
# # data.loc[data.review_body.isnull()]
#
# # data = data.drop(data[(data.review_body.isnull())].index)
#
# # data.loc[(data.review_body.isnull()), 'rev'] = "No Reviews"
#
# # Write some functions
#
# # Function to clean data
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
# # Function to return stopwords
# def stopwordcount(plist):
#     return sum(plist, [])
#
#
# data.review_body['PreCleanStopWords'] = data.review_body.apply(lambda x: stop_words(x))
# data.review_body['CleanedReview'] = data.review_body.apply(lambda x: clean_review(x))
# data.review_body['PostCleanStopWords'] = data.review_body['CleanedReview'].apply(lambda x: stop_words(x))
#
# # Function to identify stop words
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
#
# # print(data.review_body['stop_removed'][1])
#
# # print(data.review_body['clean_text'][118])
#
# for x in range(len(data)):
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
#
#     es.index(index="spark_test",
#              doc_type="test-type",
#              body={"marketplace": str(data["marketplace"][x]),
#                    "customer_id": str(data["customer_id"][x]),
#                    "review_id": str(data["review_id"][x]),
#                    "product_id": str(data["product_id"][x]),
#                    "product_parent": str(data["product_parent"][x]),
#                    "product_title": str(data["product_title"][x]),
#                    "product_sub_category": str(data["product_sub_category"][x]),
#                    "product_category": str(data["product_category"][x]),
#                    "star_rating": str(data["star_rating"][x]),
#                    "helpful_votes": str(data["helpful_votes"][x]),
#                    "total_votes": str(data["total_votes"][x]),
#                    "vine": str(data["vine"][x]),
#                    "verified_purchase": str(data["verified_purchase"][x]),
#                    "review_headline": str(data["review_headline"][x]),
#                    "review_body": str(data["review_body"][x]),
#                    "review_date": str(data["review_date"][x]),
#                    "sentimental_score": analysis.sentiment.polarity,
#                    "subjectivity": analysis.sentiment.subjectivity,
#                    "sentiment": sentiment,
#                    "review_keywords": data.review_body['stop_removed'][x],
#                    "sentimentValue": sentimentValue})