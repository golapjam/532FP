import pyspark.sql.functions as functions
import pyspark

def getStats(dataframe):
    wordCounts = wordCount(dataframe)
    smallWords = getSmallWords(wordCounts)
    midlengthWords = getMidWords(wordCounts)
    largeWords = getLargeWords(wordCounts)
    # largeWords.show()

    return wordCounts, smallWords, midlengthWords, largeWords


def wordCount(dataframe):
    wordCounts = dataframe.groupBy("words").count().orderBy(functions.desc("count")).alias("counts")

    return wordCounts

def getSmallWords(dataframe):
    smallWords = dataframe.filter(functions.length("words") < 8)

    return smallWords

def getMidWords(dataframe):
    midlengthWords = dataframe.filter(functions.length("words") > 8).filter(functions.length("words") < 15)

    return midlengthWords

def getLargeWords(dataframe):
    largeWords = dataframe.filter(functions.length("words") > 15)

    return largeWords
