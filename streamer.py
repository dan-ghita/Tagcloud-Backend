# Twitter API Crawler
# -*- coding: utf-8 -*-

'''
Author: chengjun wang
Email: wangchj04@gmail.com
Hong Kong, 2013/01/20
'''
import sys
import tweepy
import codecs
from time import clock

'''OAuth Authentication'''
consumer_key="xcEI4sb...fi6AzBQ"
consumer_secret="5nfeG8...jUX8nU2pafr4hU"
access_token="37595783-Fazh...8fPaH5IaTlz7y"
access_token_secret="fyqUf5...YijKwvQe3I"

auth1 = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth1.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth1)

'''
# Note: Had you wanted to perform the full OAuth dance instead of using
# an access key and access secret, you could have uses the following
# four lines of code instead of the previous line that manually set the
# access token via auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET).
# auth_url = auth.get_authorization_url(signin_with_twitter=True)
# webbrowser.open(auth_url)
# verifier = raw_input('PIN: ').strip()
# auth.get_access_token(verifier)
'''

# file = open("C:/Python27/twitter/mydata6.csv",'wb') # save to csv file

print (api.me().name) # api.update_status('Updating using OAuth authentication via Tweepy!')

start = clock()
print start

'''Specify the stream'''

class StreamListenerChengjun(tweepy.StreamListener):

    def on_status(self, status):
        try:
            tweet = status.text.encode('utf-8')
            tweet = tweet.replace('\n', '\\n')
            user = status.author.screen_name.encode('utf-8')
            userid = status.author.id
            time = status.created_at
            source = status.source
            tweetid = status.id
            timePass = clock()-start
            if timePass%60==0:
                print "I have been working for", timePass, "seconds."
            if not ('RT @' in tweet) :    # Exclude re-tweets
                print >>file, "%s,%s,%s,%s,|%s|,%s" % (userid, user, time, tweetid, tweet, source)

        except Exception, e:
            print >> sys.stderr, 'Encountered Exception:', e
            pass

    def on_error(self, status_code):
        print 'Error: ' + repr(status_code)
        return True # False to stop

    def on_delete(self, status_id, user_id):
        """Called when a delete notice arrives for a status"""
        print "Delete notice for %s. %s" % (status_id, user_id)
        return

    def on_limit(self, track):
        """Called when a limitation notice arrvies"""
        print "!!! Limitation notice received: %s" % str(track)
        return

    def on_timeout(self):
        print >> sys.stderr, 'Timeout...'
        time.sleep(10)
        return True

'''Link the tube with tweet stream'''

consumer_key = 'VTWaWm9crvGaUVStBrb4nbuE0'
consumer_secret = 'lxVaUHIRcHUPyt7iJUMx4XIEHmVZuRWiLX09GyDO8lyDSmhuc7'
access_token = '2863729390-sxlaAEwrRHrPEgyOjpT7COojrvoXminiC7h1lNv'
access_token_secret = 'zCtxkQIx5yHxzfDh4A4NOcXpoV6PUmSboqHlRjtN8bwoJ'

streamTube = tweepy.Stream(auth=auth1, listener=StreamListenerChengjun(), timeout= 300)# https://github.com/tweepy/tweepy/issues/83 # setTerms = ['good', 'goodbye', 'goodnight', 'good morning'] # streamer.filter(track = setTerms)
streamTube.sample()

# file.close()
pass

timePass = time.clock()-start
print timePass
