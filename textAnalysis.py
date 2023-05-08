import requests
import json
from pyspark.sql import SparkSession
import os
from cleaning import cleanData
from statistics import getStats
# -*- coding: utf-8 -*-

def mainLoop():
    authors = input('Enter Authors\' Names Separated by Comma (First Last, First Last): ').split()
    while len(authors) != 4:
        print('Please use format First Last, First Last exactly (drop middle names).')
        authors = input('Enter Authors\' Names Separated by Comma (First Last, First Last): ').split()
    corpus = getBooks(authors)
    while len(corpus) != 2:
        print('Please choose another pair of authors for comparison')
        print('(Hint: most books available are those in public domain,\n so try searching for authors published before 1923)')
        authors = input('Enter Authors\' Names Separated by Comma (First Last, First Last): ').split()
        corpus = getBooks(authors)
    spark = SparkSession.builder.appName('TextAnalysis').getOrCreate()
    outF1 = open('corpus1.txt', 'w', encoding='utf-8-sig')
    outF1.write(corpus[0])
    outF1.close()
    outF2 = open('corpus2.txt', 'w', encoding='utf-8-sig')
    outF2.write(corpus[1])
    outF2.close()
    logData1 = spark.read.text('corpus1.txt').cache()
    logData2 = spark.read.text('corpus2.txt').cache()
    numAs = logData1.filter(logData1.value.contains('a')).count()
    print('Lines with a: %i\n' % numAs)
    logData1 = cleanData(logData1)
    logData2 = cleanData(logData2)
    print(f'Stats for {authors[0]}:')
    statData1 = getStats(logData1)
    print(f'Stats for {authors[1]}:')
    statData2 = getStats(logData2)
    spark.stop()
    if os.path.exists('corpus.txt'):
        os.remove('corpus.txt')


def getBooks(authors):
    res = []
    a1 = authors[:2]
    a1[1] = a1[1][:-1]
    a2 = authors[2:]
    query1 = 'https://gutendex.com/books?search=complete%20works%20' + a1[0].lower() + '%20' + a1[1].lower()
    query2 = 'https://gutendex.com/books?search=complete%20works%20' + a2[0].lower() + '%20' + a2[1].lower()
    print(query1)
    print(query2)
    for q, a in zip([query1, query2], [a1, a2]):
        author = '' + a[0] + ' ' + a[1]
        try:
            req = requests.get(q)
            req.raise_for_status()
        except requests.exceptions.HTTPError as e:
            SystemError(e)
        except requests.exceptions.RequestException as e:
            SystemExit(e)
        asJSON = req.json()
        req.close()
        if asJSON['count'] == 0:
            print(f'Could not find complete works for \'{author}\', trying individual works...')
            query = 'https://gutendex.com/books/?search=' + a[0] + '%20' + a[1]
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
                print(f'Could not find works for \'{author}\'')
                return res
            else:
                txt = asJSON['results'][0]['formats']['text/plain']
            try:
                req = requests.get(txt)
                req.raise_for_status()
            except requests.exceptions.HTTPError as e:
                SystemError(e)
            except requests.exceptions.RequestException as e:
                SystemExit(e)
            print(f'Found partial works for \'{author}\'')
            title = asJSON['results'][0]['title']
            print(f'Using data from \'{title}\' by \'{author}\'')
            res.append(req.text)
        else:
            txt = asJSON['results'][0]['formats']['text/plain']
            try:
                req = requests.get(txt)
                req.raise_for_status()
            except requests.exceptions.HTTPError as e:
                SystemError(e)
            except requests.exceptions.RequestException as e:
                SystemExit(e)
            print(f'Found complete works for \'{author}\'')
            res.append(req.text)
    return res
    
if __name__ == '__main__':
    mainLoop()
