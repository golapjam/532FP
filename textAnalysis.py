import requests
import json
from pyspark.sql import SparkSession
import os
from cleaning import cleanData
from statistics import getStats
# -*- coding: utf-8 -*-

def mainLoop():
    authors = input('Enter 2 Authors\' Names Separated by Comma (First Last, First Last): ').split()
    while len(authors) != 4:
        print('Please use format First Last, First Last exactly (drop middle names).')
        authors = input('Enter 2 Authors\' Names Separated by Comma (First Last, First Last): ').split()
    corpus = getBooks(authors)
    while len(corpus) != 2: 
        print('Please choose another pair of authors for comparison')
        print('(Hint: most books available are those in public domain,\n so try searching for authors published before 1923)')
        authors = input('Enter Authors\' Names Separated by Comma (First Last, First Last): ').split()
        corpus = getBooks(authors)
    # now that we have our corpus we can begin SparkSession to clean and calculate
    spark = SparkSession.builder.appName('TextAnalysis').getOrCreate()
    # right now corpus is list of two big lines of text ['book...', 'book...']
    # we want each row of our DF to be a line of text within the book, so we must write to .txt file
    # in order to convert \n to newline
    outF1 = open('corpus1.txt', 'w', encoding='utf-8-sig')
    outF1.write(corpus[0])
    outF1.close()
    outF2 = open('corpus2.txt', 'w', encoding='utf-8-sig')
    outF2.write(corpus[1])
    outF2.close()
    # read in our data to Spark DF
    logData1 = spark.read.text('corpus1.txt').cache()
    logData2 = spark.read.text('corpus2.txt').cache()
    # clean data
    logData1 = cleanData(logData1)
    logData2 = cleanData(logData2)
    # calculate stats and show visualizations
    print(f'Stats for {authors[0]}:')
    statData1 = getStats(logData1)
    print(f'Stats for {authors[1]}:')
    statData2 = getStats(logData2)
    spark.stop()
    # remove temporary corpus files
    if os.path.exists('corpus1.txt'):
        os.remove('corpus1.txt')
    if os.path.exists('corpus2.txt'):
        os.remove('corpus2.txt')


def getBooks(authors):
    """
    Download corpus for authors through gutendex API requests

    -----------------
    authors: list
        The two authors whose work we want to fetch

    -----------------
    Return list of text
    """
    res = [] # for storing found text
    a1 = authors[:2]
    a1[1] = a1[1][:-1]
    a2 = authors[2:]
    # build query for gutendex request
    query1 = 'https://gutendex.com/books?search=complete%20works%20' + a1[0].lower() + '%20' + a1[1].lower()
    query2 = 'https://gutendex.com/books?search=complete%20works%20' + a2[0].lower() + '%20' + a2[1].lower()
    # request loop for (query, author) pairs
    # first try to find 'Complete Works of X' which would give largest dataset
    # if no results, search for any books by author and pull first result
    for q, a in zip([query1, query2], [a1, a2]):
        author = '' + a[0] + ' ' + a[1]
        try:
            req = requests.get(q)
            req.raise_for_status()
        except requests.exceptions.HTTPError as e:
            SystemError(e)
        except requests.exceptions.RequestException as e:
            SystemExit(e)
        # convert to JSON for accessibility
        asJSON = req.json()
        req.close()
        if not validateAuthor(asJSON, author):
            print(f'Could not find complete works for \'{author}\', trying individual works...')
            query = 'https://gutendex.com/books/?search=' + a[0] + '%20' + a[1]
            # now try for any books
            try:
                req = requests.get(query)
                req.raise_for_status()
            except requests.exceptions.HTTPError as e:
                SystemError(e)
            except requests.exceptions.RequestException as e:
                SystemExit(e)
            asJSON = req.json()
            req.close()
            if not validateAuthor(asJSON, author):
                print(f'Could not find works for \'{author}\'')
                return res # return to main function to try different author
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

def validateAuthor(results, author):
    if results['count'] == 0:
        return False
    checkAuth = author.split()
    resAuth = results['results'][0]['authors'][0]['name'].split()
    return resAuth[0][:-1].lower() == checkAuth[1] and resAuth[1].lower() == checkAuth[0]

if __name__ == '__main__':
    mainLoop()
