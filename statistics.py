import pyspark
import nltk
import pyspark.sql.functions as functions
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from nltk.corpus import cmudict
from pyspark.sql.types import IntegerType
from pyspark.sql import functions
from pyspark.sql.functions import udf, sum, count
from math import ceil
import time




def getStats(dataframe):
    """
    Driver method for calculating statistics

    dataframe: pyspark.sql.DataFrame
        The corpus of an author, each row is a line of text

    Returns 
    """
    wc_st_time = time.time()
    wordCounts = wordCount(dataframe)
    wc_ed_time = time.time() -  wc_st_time
    sm_st_time = time.time()
    smallWords = getSmallWords(wordCounts)
    sm_ed_time = time.time() - sm_st_time
    ml_st_time = time.time()
    midlengthWords = getMidWords(wordCounts)
    ml_ed_time = time.time() - ml_st_time
    lg_st_time = time.time()
    largeWords = getLargeWords(wordCounts)
    lg_ed_time = time.time() - lg_st_time

    wdc_st_time = time.time()
    # word cloud
    wordCloud(wordCounts)
    wdc_ed_time = time.time() - wdc_st_time

    cht_st_time = time.time()
    # word distribution
    plot_word_distribution_chart(smallWords, midlengthWords, largeWords)
    cht_end_time = time.time() - cht_st_time

    cli_st_time = time.time()
    # coleman liau index
    c_l_idx = calc_coleman_liau(wordCounts)
    cli_ed_time = time.time() - cli_st_time

    fgi_st_time = time.time()
    # calculate fog index
    fog_index = fogIndex(wordCounts)
    fgi_ed_time = time.time() - fgi_st_time

    smg_time = time.time()
    # get smog grade
    smog_grade = calc_smog_grade(wordCounts)
    smg_time = time.time() - smg_time

    print(f"\n\nWord Count Time: {wc_ed_time} seconds")
    print(f"Small Words count Time: {sm_ed_time} seconds")
    print(f"Medium words count Time: {ml_ed_time} seconds")
    print(f"Large Word Time: {lg_ed_time} seconds")
    print(f"Word Cloud Time: {wdc_ed_time} seconds")
    print(f"Chart creation Time: {cht_end_time} seconds")
    print(f"Coleman-Liau calculation Time: {cli_ed_time} seconds")
    print(f"Fog index calculation Time: {fgi_ed_time} seconds")
    print(f"SMOG Grade calculation Time: {smg_time} seconds\n\n")


    return wordCounts, smallWords, midlengthWords, largeWords

def wordCount(dataframe):
    """
    Calculate word count through DF functions
    
    dataframe: pyspark.sql.DataFrame
        The corpus of an author, each row is a line of text
    
    Returns DataFrame of word counts
    """
    wordCounts = dataframe.groupBy("words").count().orderBy(functions.desc("count")).alias("counts").cache()
    print(f"Word Counts : ")
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
    print(f"Small Words (length <7) :")
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
    print(f"Medium Words ( 7 < length <14) :")
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
    print(f"Large Words (length > 15):")
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
    print(f"Total count of small, medium, and lare words in the text: ")
    cat_count_df.show()

    # convert pyspark dataframe to pandas
    cat_count_df = cat_count_df.toPandas()
    cat_count_df["Word length"] = ["Small", "medium", "Large"]

    # Plot pie chart
    plt.pie(cat_count_df["counts"], labels=cat_count_df["Word length"])
    plt.title("Use of words according to their length")
    plt.show()

def wordCloud(dataframe):
    # Concatenate all words
    word_df = dataframe.toPandas()

    # gen wordcloud
    wordcloud = WordCloud(width=800, height=400, background_color="white").generate_from_frequencies(word_df.set_index("words")['count'])

    # plot
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    plt.savefig(f"outputs/wordCloud_{time.time()}.png")
    plt.show()

def fogIndex(dataframe):
    total_words = dataframe.select(functions.sum("count")).first()[0]
    total_sentences = dataframe.select(functions.count("count")).first()[0]

    words_per_sentence = total_words / total_sentences

    complex_words = dataframe.filter(functions.length("words") > 7).select(functions.sum("count")).first()[0]

    complex_words_pct = (complex_words/total_words) * 100

    fog_idx = 0.4 * (words_per_sentence + complex_words_pct)

    print(f"Gunning fog index : {fog_idx}")

    return fog_idx

def calc_coleman_liau(dataframe):
    total_words = dataframe.select(functions.sum("count")).first()[0]
    total_sentences = dataframe.select(functions.count("count")).first()[0]

    words_per_sentence = total_words/ total_sentences

    lett_pct = dataframe.select(functions.sum(functions.length("words") * functions.col("count"))).first()[0]/total_words

    c_l_idx = 0.058 * (lett_pct*100) - 0.296 * (words_per_sentence*100) - 16

    print(f"Coleman-Liau Index: {c_l_idx}")

    return c_l_idx

def calc_smog_grade(dataframe):
    # calc sentences
    total_sentences = dataframe.select(functions.count("count")).first()[0]

    polysyl_words = dataframe.filter(functions.length("words") > 7).select(functions.sum("count")).first()[0]

    smog_grade = (polysyl_words * (30 / total_sentences)) ** 0.5 + 3

    print(f"SMOG Grade: {smog_grade}")

    return smog_grade

