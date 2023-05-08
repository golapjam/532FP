import pyspark
import pyspark.sql.functions as functions
import matplotlib.pyplot as plt


def getStats(dataframe):
    wordCounts = wordCount(dataframe)
    smallWords = getSmallWords(wordCounts)
    midlengthWords = getMidWords(wordCounts)
    largeWords = getLargeWords(wordCounts)

    plot_word_distribution_chart(smallWords, midlengthWords, largeWords)

    return wordCounts, smallWords, midlengthWords, largeWords

def wordCount(dataframe):
    wordCounts = dataframe.groupBy("words").count().orderBy(functions.desc("count")).alias("counts").cache()
    wordCounts.show()
    return wordCounts

def getSmallWords(dataframe):
    smallWords = dataframe.filter(functions.length("words") <= 7).cache()
    smallWords.show()
    return smallWords

def getMidWords(dataframe):
    midlengthWords = dataframe.filter(functions.length("words") > 7).filter(functions.length("words") <= 14).cache()
    midlengthWords.show()
    return midlengthWords

def getLargeWords(dataframe):
    largeWords = dataframe.filter(functions.length("words") > 14)
    largeWords.show()
    return largeWords

def plot_word_distribution_chart(smallWords, midlengthWords, largeWords):

    # combine aggregate of word counts by to their length
    smallWords_count = smallWords.agg({"count": "sum"})
    midlengthWords_count = midlengthWords.agg({"count": "sum"})
    largeWords_count = largeWords.agg({"count": "sum"})

    cat_count_df = smallWords_count.union(midlengthWords_count).union(largeWords_count)
    cat_count_df = cat_count_df.select(functions.col("sum(count)").alias("counts"))
    cat_count_df.show()

    # convert pyspark dataframe to pandas
    cat_count_df = cat_count_df.toPandas()
    cat_count_df["Word length"] = ["Small", "medium", "Large"]

    # Plot pie chart
    plt.pie(cat_count_df["counts"], labels=cat_count_df["Word length"])
    plt.title("Use of words according to their length in Shakespeare's works")
    plt.show()
