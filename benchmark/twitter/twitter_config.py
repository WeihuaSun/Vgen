from pathlib import Path


class Users:
    table = "users"
    columns = ["name", "info"]


class Tweet:
    table = "tweet"
    columns = ["author", "data"]


class LastTweet:
    table = "last_tweet"
    columns = ["last_tweet_id"]


class FollowList:
    table = "follow_list"
    columns = ["data"]


class Followers:
    table = "followers"
    columns = ["time", "follow"]


class Following:
    table = "following"
    columns = ["time", "follow"]


class Config:
    # data
    num_users = 1000
    num_tweets = 10
    # load
    num_loaders = 16

    # transaction weight
    weight_follow = 40
    weight_new_tweet = 20
    weight_show_follow = 10
    weight_show_tweet = 20
    weight_time_line = 10

    # delay time
    delay_time = 3*1e9  # ns

    # run
    zipf_const1 = 2
    zipf_const2 = 1.1

    num_transactions = 10000
    num_sessions = 25
    num_monkeys = 4
    num_terminals = 25

    # path

    @classmethod
    def output_path(cls, session_id):
        return Path(f"./output/ctwitter_{cls.num_transactions}/{session_id}.log")


FOLLOW = "follow"
NEW_TWEET = "new_tweet"
SHOW_FOLLOW = "show_follow"
SHOW_TWEET = "show_tweets"
TIMELINE = "timeline"
