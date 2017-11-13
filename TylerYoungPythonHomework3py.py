# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

###AUTHOR: Tyler Young

import pymysql
import getpass
import re


database = 'tyoung12'
user = 'tyoung12'
pw = getpass.getpass();

# 1. Open database connection
db = pymysql.connect(host='cs.elon.edu',
                     db= database,
                     user= user,
                     passwd= pw,
                     port=3306,
                     charset='utf8mb4')
                    
cursor = db.cursor()

#query to select each tweet from the table
queryTweets = "SELECT * FROM ferguson_tweets;"
cursor.execute(queryTweets)

#Select each tweet from the table
tweets = cursor.fetchall()

#For each tweet, get the tweet id, tweet text, time created at, and user
for tweet in tweets:
    tid = tweet[0]
    ttext = tweet[1]
    tcreated_at = tweet[2]
    tuser = tweet[3]
    
    #find each hashtag in the tweet and insert into the ferguson_tweets_hashtags
    #table.  If an error occurs when executing, do not commit the insert query
    hashtagSearch = re.findall('#[\w\S]*', ttext)
    for tag in hashtagSearch:
        hashtagInsertQuery = 'INSERT INTO ferguson_tweets_hashtags(tweet_id,\
        hashtag_text) VALUES(%s,%s)'
        try:
            cursor.execute(hashtagInsertQuery,(tid, tag))
            db.commit()
        except:
            db.rollback()
            print('Unable to insert hashtag')
            
    #find each mention in the tweet and insert into the ferguson_tweets_mentions
    #table.  If an error occurs when executing, do not commit the insert query
    mentionSearch = re.findall('@[^:^\s]*', ttext)
    for mention in mentionSearch:
        mentionInsertQuery = 'INSERT INTO ferguson_tweets_mentions(tweet_id,\
        mention_text) VALUES (%s,%s)'
        try:
            cursor.execute(mentionInsertQuery, (tid, mention))
            db.commit()
        except:
            db.rollback()
            print('Unable to insert mention')
            
    #find each url in the tweet and insert into the ferguson_tweets_urls
    #table.  If an error occurs when executing, do not commit the insert query
    urlSearch = re.findall('htt[\w\S]*', ttext)
    for url in urlSearch:
        urlInsertQuery = 'INSERT INTO ferguson_tweets_urls(tweet_id, url_text)\
        VALUES (%s, %s)'
        try:
            cursor.execute(urlInsertQuery, (tid, url))
            db.commit()
        except:
            db.rollback()
            print('Unable to insert URL')
            
      
# 5. Close connection
db.close()