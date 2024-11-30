
import threading
import base64

from utils import RandomUtils, SharedInt, encode_key, encode_value, set_bit_map_at, pack_key, str_to_long
from benchmark.twitter.twitter_config import *

key_set = set()


class Loader(threading.Thread):
    def __init__(self, conn, s_user_id: SharedInt):
        super().__init__()
        self.conn = conn
        self.ru = RandomUtils()
        self.s_user_id = s_user_id

    def load_following(self, src_id, dest_id):
        self.conn.begin()
        key = encode_key(src_id, dest_id)
        value = encode_value(Following.columns, "no-time", True)
        self.conn.insert(Following.table, key, value)
        self.conn.commit()

    def load_followers(self, src_id, dest_id):
        self.conn.begin()
        key = encode_key(dest_id, src_id)
        value = encode_value(Followers.columns, "no-time", True)
        self.conn.insert(Followers.table, key, value)
        self.conn.commit()

    def load_follow_list(self, user_id):
        self.conn.begin()
        key = encode_key(user_id)
        bytes = bytearray(2000)
        set_bit_map_at(bytes, user_id-1)  # follow self
        data = base64.b64encode(bytes).decode('utf-8')
        value = encode_value(FollowList.columns, data)
        self.conn.insert(FollowList.table, key, value)
        self.conn.commit()

    def load_last_tweet(self, tweet_id, user_id):
        self.conn.begin()
        key = encode_key(user_id)
        value = encode_value(LastTweet.columns, tweet_id)
        self.conn.insert(LastTweet.table, key, value)
        self.conn.commit()

    def load_tweet(self, user_id):
        self.conn.begin()
        num_tweets = Config.num_tweets
        for i in range(num_tweets):
            data = self.ru.get_string(100, 100)
            tweet_id = (user_id-1)*num_tweets+i+1
            key = encode_key(tweet_id)
            value = encode_value(Tweet.columns, user_id, data)
            self.conn.insert(Tweet.table, key, value)
        self.conn.commit()
        return tweet_id

    def load_user(self, user_id):
        self.conn.begin()
        name = self.ru.get_string(10, 10)
        info = self.ru.get_string(200, 200)
        key = encode_key(user_id)
        value = encode_value(Users.columns, name, info)
        self.conn.insert(Users.table, key, value)
        self.conn.commit()

    def run(self):
        while True:
            user_id = self.s_user_id.increment()
            if user_id == -1:
                break
            self.load_user(user_id)
            tweet_id = self.load_tweet(user_id)
            self.load_last_tweet(tweet_id, user_id)
            self.load_follow_list(user_id)
            self.load_following(user_id, user_id)
            self.load_followers(user_id, user_id)
