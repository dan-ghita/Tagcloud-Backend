__author__ = 'dan'

import redis
import sys
import json
import re
import tweepy
import time

class StreamListener(tweepy.StreamListener):

    def __init__(self, duration = 0):
        super(StreamListener, self).__init__()
        self.text = ""
        self.start_time = time.time()
        self.duration = duration

    def on_data(self, data):

        elapsedTime = time.time() - self.start_time

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


def fetch_data(duration):

    with open('OAuth.json') as stream:
        keys = dict(json.load(stream))

    consumer_key = keys["consumer_key"]
    consumer_secret = keys["consumer_secret"]
    access_token = keys["access_token"]
    access_token_secret = keys["access_token_secret"]

    lsn = StreamListener(duration)
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    print("Reading data...")
    stream = tweepy.Stream(auth, lsn)

    stream.sample(False, languages = ['en'])
    return lsn.text


def get_stopwords():
    with open('stopwords.txt') as inputFile:
        return inputFile.read()

def parse_data(text, stopwords, word_count):

    text = re.split('[,.:@\/_ ?|#&*><;"\n\t]', text)

    for word in text:
        word = word.lower()
        if word not in stopwords:
            redisCont.incr(word, 1)

    for key in redisCont.keys():
        word_count.append({"word": key.decode('UTF-8'), "count": int(redisCont.get(key).decode('UTF-8'))})

if __name__ == '__main__':

    if sys.argv.__len__() == 3:
        # Should check if arguments are numbers
        duration = int(sys.argv[1])
        nr_of_words = int(sys.argv[2])
    else:
        print("Error: Bad number of arguments")
        sys.exit(1)

    # Initialise redis container an connect to server
    redisCont = redis.Redis(host='redis', port=6379, db=0)
    # Clear redis container
    redisCont.flushall()

    stopwords = get_stopwords()
    # Fetch twitter samples for "duration" seconds
    data = fetch_data(duration)

    # Initialise word_count and count words in data
    word_count = []
    parse_data(data, stopwords, word_count)

    # Sort words by count
    word_count = sorted(word_count, key = lambda k: k['count'], reverse = True)

    # Keep first "nr_of_words" words and aggregate the rest into the word "other"
    if nr_of_words < len(word_count):
        # Compress words from nr_of_words to len(word_count) into word_count[nr_of_words]
        word_count[nr_of_words]["word"] = "other"
        for i in range(nr_of_words + 1, len(word_count)):
            word_count[nr_of_words]["count"] += word_count[i]["count"]

        word_count = word_count[:nr_of_words + 1]

    json = json.dumps(word_count, separators=(', ', ': '), indent = 4, ensure_ascii=False)
    

    print(json)


