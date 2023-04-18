import string
import pyspark.sql.functions as functions
import re
from nltk.corpus import stopwords
from pyspark.sql.functions import udf
from pyspark.sql.types import *



def cleanData(dataframe):
    # convert all to lowercase
    dataframe = dataframe.select(functions.lower("value").alias("value"))
    # split words
    dataframe = dataframe.withColumn("value", functions.regexp_replace("value", "—", " "))
    dataframe = dataframe.select(functions.split("value", " ").alias("lines"))
    # explode dataframe
    dataframe = dataframe.select(functions.explode("lines").alias("words"))
    # remove unwanted characters
    dataframe = dataframe.withColumn("words", udf_removeUnwanted(dataframe["words"]))
    # remove stopwords
    dataframe = dataframe.withColumn("words", udf_removeStopwords(dataframe["words"]))
    # remove empty rows form Dataframe
    dataframe = dataframe.filter("words != ''")


    return dataframe


def removeUnwanted(dataframe_row):
    # remove punctuations
    punctuation_regex = re.compile('[%s]' % re.escape(string.punctuation))
    # remove numericals
    numerical_regex = re.compile('(\\d+)')
    # remove links
    url_regex = re.compile("https?://(www.)?\w+\.\w+(/\w+)*/?")

    dataframe_row = punctuation_regex.sub(" ", dataframe_row)
    dataframe_row = numerical_regex.sub(" ", dataframe_row)
    dataframe_row = url_regex.sub(" ", dataframe_row)

    return dataframe_row

def removeStopwords(dataframe_row):
    stopwords_set = set(stopwords.words("english"))
    ret_str = str()

    for word in dataframe_row.split():
        if not word in stopwords_set:
            ret_str = word + ' '

    return ret_str


udf_removeUnwanted = udf(removeUnwanted, StringType())
udf_removeStopwords = udf(removeStopwords, StringType())