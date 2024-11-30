import threading
from utils import ZipfRangeGenerator, random_bool, make_timestamp, RandomUtils
from benchmark.terminal import Terminal
from benchmark.twitter.twitter_config import *
import random


class TwitterTerminal(Terminal):
    def __init__(self, generator):
        super().__init__()
        self.generator = generator

    def generate(self):
        trans_due = self.trans_end + Config.delay_time
        chance = random.randint(1, 100)
        if chance <= Config.weight_follow:
            self.ttype = FOLLOW
            self.record = self.generator.gen_follow()
            return trans_due
        chance -= Config.weight_follow
        if chance <= Config.weight_new_tweet:
            self.ttype = NEW_TWEET
            self.record = self.generator.gen_new_tweet()
            return trans_due
        chance -= Config.weight_new_tweet
        if chance <= Config.weight_show_follow:
            self.ttype = SHOW_FOLLOW
            self.record = self.generator.gen_show_follow()
            return trans_due
        chance -= Config.weight_show_follow
        if chance <= Config.weight_show_tweet:
            self.ttype = SHOW_TWEET
            self.record = self.generator.gen_show_tweet()
            return trans_due
        else:
            self.ttype = TIMELINE
            self.record = self.generator.gen_time_line()
            return trans_due

    def finish_time(self):
        # assume this time is later than each transaction's due time
        return self.trans_end + 3*Config.delay_time


class TwitterGenerator:
    def __init__(self):
        self.zipf1 = ZipfRangeGenerator(
            Config.zipf_const1, 1, Config.num_users)  # 2
        self.zipf2 = ZipfRangeGenerator(
            Config.zipf_const2, 1, Config.num_users)  # 1.1
        self.rand_utils = RandomUtils()
        self.tweet_id = Config.num_users*Config.num_tweets
        self.lock = threading.Lock()

    def _tweet_id_inc(self):
        with self.lock:
            self.tweet_id += 1

    def gen_follow(self):
        return Follow(self.zipf1, self.zipf2)

    def gen_new_tweet(self):
        self._tweet_id_inc()
        return NewTweet(self.tweet_id, self.zipf1, self.rand_utils)

    def gen_show_follow(self):
        return ShowFollow(self.zipf1)

    def gen_show_tweet(self):
        return ShowTweet(self.zipf1)

    def gen_time_line(self):
        return Timeline(self.zipf1)


class Follow:
    def __init__(self, zipf1: ZipfRangeGenerator, zipf2: ZipfRangeGenerator):
        self.src_id = zipf1.next_value()
        self.dest_id = zipf2.next_value()
        self.follow = random_bool()
        self.time = make_timestamp()


class NewTweet:
    def __init__(self, tweet_id, zipf: ZipfRangeGenerator, ru: RandomUtils):
        self.user_id = zipf.next_value()
        self.tweet_id = tweet_id
        self.data = ru.get_string(120, 120)


class ShowFollow:
    def __init__(self, zipf: ZipfRangeGenerator):
        self.user_id = zipf.next_value()


class ShowTweet:
    def __init__(self, zipf: ZipfRangeGenerator):
        self.user_id = zipf.next_value()


class Timeline:
    def __init__(self, zipf: ZipfRangeGenerator):
        self.user_id = zipf.next_value()
