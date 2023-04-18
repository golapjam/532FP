#from gutenberg import GutenbergAPI
import requests
import json
from pyspark.sql import SparkSession
import os
from cleaning import cleanData
from statistics import getStats

def mainLoop():
    author = input('Enter Author Name (First Last): ')
    author = author.split()
    corpus = getBooks(author)
    spark = SparkSession.builder.appName('TextAnalysis').getOrCreate()
    with open('corpus.txt', 'w', encoding='utf-8-sig') as outF:
        outF.write(corpus)
    logData = spark.read.text('corpus.txt').cache()
    numAs = logData.filter(logData.value.contains('a')).count()
    print('Lines with a: %i\n' % numAs)
    if os.path.exists('corpus.txt'):
        os.remove('corpus.txt')
    logData = cleanData(logData)
    statData = getStats(logData)
    spark.stop()


def getBooks(author):
    query = 'https://gutendex.com/books?search=complete%20works%20' + author[0].lower() + '%20' + author[1].lower()
    print(query)
    try:
        req = requests.get(query)
        req.raise_for_status()
    except requests.exceptions.HTTPError as e:
        SystemError(e)
    except requests.exceptions.RequestException as e:
        SystemExit(e)
    asJSON = req.json()
    req.close()
    if asJSON['count'] == 0:
        print('Could not find complete works, trying individual works...\n')
        query = 'https://www.gutendex.com/books?search=' + author[0] + '%20' + author[1]
        try:
            req = requests.get(query)
            req.raise_for_status()
        except requests.exceptions.HTTPError as e:
            SystemError(e)
        except requests.exceptions.RequestException as e:
            SystemExit(e)
        asJSON = req.json()
        req.close()
        if asJSON['count'] == 0:
            print('Could not find works for author\n')
            return
    else:
        txt = asJSON['results'][0]['formats']['text/plain']
        try:
            req = requests.get(txt)
            req.raise_for_status()
        except requests.exceptions.HTTPError as e:
            SystemError(e)
        except requests.exceptions.RequestException as e:
            SystemExit(e)
        return req.text
    
if __name__ == '__main__':
    mainLoop()
