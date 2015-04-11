__author__ = 'dan'

import redis
import sys
import json
import re
import tweepy
import time

class StdOutListener(tweepy.StreamListener):

    def __init__(self, duration = 0):
        super(StdOutListener, self).__init__()
        self.text = ""
        self.startTime = time.time()
        self.duration = duration

    def on_data(self, data):

        elapsedTime = time.time() - self.startTime

        if elapsedTime > duration:
            # Stop fetching data
            return False

        decoded = dict(json.loads(data))

        if "text" in decoded:
            self.text += decoded["text"]

        # Keep fetching
        return True

    def on_error(self, status):
        print ("error: ", status)
        return False

def fetchData(duration):

    with open('OAuth.json') as stream:
        keys = dict(json.load(stream))

    consumer_key = keys["consumer_key"]
    consumer_secret = keys["consumer_secret"]
    access_token = keys["access_token"]
    access_token_secret = keys["access_token_secret"]

    lsn = StdOutListener(duration)
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    print("Reading data...")
    stream = tweepy.Stream(auth, lsn)

    stream.sample(False, languages = ['en'])
    return lsn.text


def getStopwords():
    with open('stopwords.txt') as inputFile:
        return inputFile.read()

def parseData(text, stopwords, wordCount):

    text = re.split('[,.:@/\_ -?|#&*><;"\n\t]', text)

    for word in text:
        word = word.lower()
        if word not in stopwords:
            redisCont.incr(word, 1)

    for key in redisCont.keys():
        wordCount.append({"word": key.decode('UTF-8'), "count": int(redisCont.get(key).decode('UTF-8'))})

if __name__ == '__main__':


    if sys.argv.__len__() == 3:
        duration = int(sys.argv[1])
        nrOfWords = int(sys.argv[2])
    else:
        print("Error: Bad number of arguments")
        sys.exit(1)

    redisCont = redis.Redis(host='redis', port=6379, db=0)
    redisCont.flushall()

    stopwords = getStopwords()
    data = fetchData(duration)

    wordCount = []
    parseData(data, stopwords, wordCount)

    wordCount = sorted(wordCount, key = lambda k: k['count'], reverse = True)

    if nrOfWords < len(wordCount):
        # Compress words from nrOfWords to len(wordCount) into wordCount[nrOfWords]
        wordCount[nrOfWords]["word"] = "other"
        for i in range(nrOfWords + 1, len(wordCount)):
            wordCount[nrOfWords]["count"] += wordCount[i]["count"]

        wordCount = wordCount[:nrOfWords + 1]

    json = json.dumps(wordCount, separators=(', ', ': '))

    print(json)


