import pyspark
import pyspark.sql.functions as functions
import matplotlib.pyplot as plt


def getStats(dataframe):
    """
    Driver method for calculating statistics

    dataframe: pyspark.sql.DataFrame
        The corpus of an author, each row is a line of text

    Returns 
    """
    wordCounts = wordCount(dataframe)
    smallWords = getSmallWords(wordCounts)
    midlengthWords = getMidWords(wordCounts)
    largeWords = getLargeWords(wordCounts)

    plot_word_distribution_chart(smallWords, midlengthWords, largeWords)

    return wordCounts, smallWords, midlengthWords, largeWords

def wordCount(dataframe):
    """
    Calculate word count through DF functions
    
    dataframe: pyspark.sql.DataFrame
        The corpus of an author, each row is a line of text
    
    Returns DataFrame of word counts
    """
    wordCounts = dataframe.groupBy("words").count().orderBy(functions.desc("count")).alias("counts").cache()
    wordCounts.show()
    return wordCounts

def getSmallWords(dataframe):
    """
    Filter small words through DF functions
    (small defined as 1 <= len <= 7)
    
    dataframe: pyspark.sql.DataFrame
        The corpus of an author, each row is a line of text
    
    Returns DataFrame of small words
    """
    smallWords = dataframe.filter(functions.length("words") <= 7).cache()
    smallWords.show()
    return smallWords

def getMidWords(dataframe):
    """
    Filter mid-length words through DF functions
    (mid defined as 7 < len <= 14)
    
    dataframe: pyspark.sql.DataFrame
        The corpus of an author, each row is a line of text
    
    Returns DataFrame of mid-length words
    """
    midlengthWords = dataframe.filter(functions.length("words") > 7).filter(functions.length("words") <= 14).cache()
    midlengthWords.show()
    return midlengthWords

def getLargeWords(dataframe):
    """
    Filter large words through DF functions
    (large defined as 14 < len)
    
    dataframe: pyspark.sql.DataFrame
        The corpus of an author, each row is a line of text
    
    Returns DataFrame of large words
    """
    largeWords = dataframe.filter(functions.length("words") > 14)
    largeWords.show()
    return largeWords

def plot_word_distribution_chart(smallWords, midlengthWords, largeWords):
    """
    Plot chart of word distributions based on counts of small, mid, and large words

    smallWords: pyspark.sql.DataFrame
        DataFrame containing only small words
    midlengthWords: pyspark.sql.DataFrame
        DataFrame containing only mid-length words
    largeWords: pyspark.sql.DataFrame
        DataFrame containing only large words
    """

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
