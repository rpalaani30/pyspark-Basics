#!/usr/bin/env python
# coding: utf-8

# In[1]:




import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json

consumer_key = ' '
consumer_secret = ' '
access_token = ''
access_secret = ''

# In[2]:


class TweetsListener(StreamListener):
  def __init__(self, csocket):
      self.client_socket = csocket
  def on_data(self, data):
      try:
          msg = json.loads( data )
          print(msg['text'].encode('utf-8'))
            #self.db.stream.insert_one(msg)
          #self.client_socket.send( msg['text'].encode('utf-8') )
          self.client_socket.send((str(msg['text']) + "\n").encode('utf-8'))
          return True
      except BaseException as e:
          print("Error on_data: %s" % str(e))
      return True
  def on_error(self, status):
      print(status)
      return True


# In[3]:


def sendData(c_socket):
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_secret)
  twitter_stream = Stream(auth, TweetsListener(c_socket))
  twitter_stream.filter(track=['Covid','covid 19','coronavirus'])


# In[ ]:


if __name__ == '__main__':
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    host = "localhost"     # Get local machine name
    port = 5555   
    s.bind((host,port))
    print("Listening on port: %s" % str(port))
    s.listen(5)
    c, addr = s.accept()
    print( "Received request from: " + str( addr ) )
    print(c)
    sendData(c)


# In[ ]:




