#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import findspark
findspark.init('/home/manasa/spark-2.4.5-bin-hadoop2.7')


# In[ ]:


import pyspark


# In[ ]:


import tweepy
from tweepy import OAuthHandler,Stream


# In[ ]:


from tweepy.streaming import StreamListener
import socket
import json


# In[ ]:


consumer_key = 'QGJH14PH3A5L3K8vaaVsj2rCt'
consumer_secret = 'sqFvxX86LtAIn5jMOLLOxr2fcldadz2erQoFCQgAW7wy2KxOyB'
access_token = '1126218015580860417-oSR9uKhB5P7Nf1zLYykxZYb1CWehEi'
access_secret = 'mpY9aO8yQhWpEp0TXfqYomiw3QW3ii9VLE6pdMCUYQOmk'


# In[ ]:


class TweetListener(StreamListener):
    def __init__(self,csocket):
        self.client_socket = csocket
        
    def on_data(self,data):
        
        try:
            msg = json.loads(data)
            print(msg['text'].encode('utf-8'))
            self.client_socket.send(msg['text'].encode('utf-8'))
            return True 
        except BaseException as e:
            print("Error",e)
            return True
            
        def on_error(self,status):
            print(status)
        return True


# In[ ]:


def sendData(c_socket):
    auth = OAuthHandler(consumer_key,consumer_secret)
    auth.set_access_token(access_token,access_secret)
    
    twitter_stream = Stream(auth,TweetListener(c_socket))
    twitter_stream.filter(track=['world'])


# In[ ]:


if __name__ == '__main__':
    s = socket.socket()
    host = '127.0.0.1'
    port = 5555
    s.bind((host,port))
    
    print('listenin on port 5555')
    s.listen(5)
    c,addr = s.accept()
    
    sendData(c)


# In[ ]:




