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

        elapsed_time = time.time() - self.start_time

        if elapsed_time > self.duration:
            # Stop fetching data
            return False

        decoded = dict(json.loads(data))

        if "text" in decoded:
            self.text += decoded["text"]

        # Keep fetching
        return True

    def on_error(self, status):
        print("error: ", status)
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

    print("Fetching data...")

    stream = tweepy.Stream(auth, lsn)

    stream.sample(False, languages = ['en'])
    return lsn.text


def get_stopwords():
    with open('stopwords.txt') as inputFile:
        return inputFile.read()


def parse_data(text, stopwords, word_count, redisCont):

    cnt = 0

    # Remove links
    text = re.sub('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
    # Split text
    text = re.split("[ \.,\-:@\/_ ?|#&*>=<;\"\n\t]", text)

    for word in text:
        word = word.lower()
        if word not in stopwords:
            # increment the score for word
            redisCont.zincrby("wordCount", word)
            cnt += 1

    return cnt


def main():

    # Parse cmd args
    if sys.argv.__len__() == 3:
        try:
            duration = int(sys.argv[1])
            nr_of_words = int(sys.argv[2])
        except:
            print("Error: main.py must receive two integers as arguments")
            sys.exit(1)
    else:
        print("Error: Bad number of arguments")
        sys.exit(1)

    # Initialise redis container an connect to server
    redisCont = redis.Redis(host='redis', port=6379, db=0)
    # Clear redis container
    redisCont.flushall()

    stopwords = set(re.split('[ \n\t]', get_stopwords()))
    # Fetch twitter samples for "duration" seconds
    data = fetch_data(duration)

    # Initialise word_count and count words in data
    word_count = []
    total_words_count = parse_data(data, stopwords, word_count, redisCont)
    
    # Get first nr_of_words words from redis
    sorted = redisCont.zrevrange("wordCount", 0, nr_of_words - 1, withscores = True)

    # Add the words into an array, decode word and count how many words we have so far
    top_words_count = 0
    for word in sorted:
        word_count.append({"word": word[0].decode('UTF-8'), "count": int(word[1])})
        top_words_count += 1
    
    # Append the "other" word to word_count and set it's count
    word_count.append({"word" : "other", "count" : total_words_count - top_words_count})

    word_count_json = json.dumps(word_count, separators=(', ', ': '), indent = 4, ensure_ascii = False)

    with open("data.json", "w+") as output:
        output.write(word_count_json)

    print("Done!")

if __name__ == '__main__':
    main()
