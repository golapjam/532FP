import unittest
from src.driver.statistics import wordCount, getSmallWords, getMidWords, getLargeWords
from src.driver.cleaning import cleanData
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession

class SparkTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession
                     .builder
                     .master("local[*]")
                     .appName("unitTests")
                     .getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_WordCount(self):
        # From https://www.opensourceshakespeare.org/statistics/:
        # Total number of words in all Shakespeare sonnets is 17,515
        # The top 1% most frequently occuring words make up 66.7% of all words
        # Top 15 words are 'the, and, i, to, of, a', etc., all removed through cleaning data
        # So total word count after cleaning should be about 50% of 17,515 (+- 10%)
        # We test for sum in interval of (7,006, 10,508)
        input_df = self.spark.read.text('C:\\Users\\jmmoo\\cs532\\532FP\\src\\tests\\testCorpus.txt').cache()
        input_df = cleanData(input_df)
        wordCDF = wordCount(input_df)
        sumWords = wordCDF.select(sum(wordCDF['count']).alias('total'))
        total = sumWords.collect()[0].asDict()['total']
        res = (total >= 7006 and total <= 10508)
        self.assertTrue(res)
    
    def test_WordDistr(self):
        # Following Zipf's Law and opensourceshakespeare, the general trend is
        # that small words appear most abundantly at ~80%, mid words follow ~15%
        # and large words appear very very infrequently
        input_df = self.spark.read.text('C:\\Users\\jmmoo\\cs532\\532FP\\src\\tests\\testCorpus.txt').cache()
        input_df = cleanData(input_df)
        wordCDF = wordCount(input_df)
        wordSDF = getSmallWords(input_df)
        wordMDF = getMidWords(input_df)
        wordLDF = getLargeWords(input_df)
        sumWords = wordCDF.select(sum(wordCDF['count']).alias('total')).collect()[0].asDict()['total']
        sumSmall = wordSDF.count()
        sumMid = wordMDF.count()
        sumLarge = wordLDF.count()
        resSmall = (sumSmall >= 0.7 * sumWords and sumSmall <= 0.9 * sumWords)
        resMid = (sumMid >= 0.075 * sumWords and sumMid <= 0.20 * sumWords)
        resLarge = (sumLarge >= 0 and sumLarge <= 0.05 * sumWords)
        print([sumSmall, sumMid, sumLarge])
        self.assertTrue(resSmall)
        self.assertTrue(resMid)
        self.assertTrue(resLarge)
        
        

if __name__ == '__main__':
    unittest.main()