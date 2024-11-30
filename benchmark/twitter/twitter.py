from database.database import DBManager
from benchmark.twitter.twitter_config import *
from benchmark.twitter.twitter_load import Loader
from benchmark.twitter.twitter_app import TwitterApp
from utils import SharedInt
from benchmark.scheduler import Scheduler
from benchmark.twitter.twitter_terminal import *
from benchmark.terminal import TerminalManager
from benchmark.application import ApplicationManager


class Twitter:
    def __init__(self):
        pass
    
    def drop_tables(self):
        conn = self.db.connect()
        conn.drop_table(Users.table)
        conn.drop_table(Tweet.table)
        conn.drop_table(LastTweet.table)
        conn.drop_table(FollowList.table)
        conn.drop_table(Following.table)
        conn.drop_table(Followers.table)
        self.db.close()

    def create_tables(self):
        self.drop_tables()
        conn = self.db.connect()
        conn.create_table(Users.table, 30, 300)
        conn.create_table(Tweet.table, 30, 300)
        conn.create_table(LastTweet.table, 30, 100)
        conn.create_table(FollowList.table, 30, 3000)
        conn.create_table(Following.table, 30, 100)
        conn.create_table(Followers.table, 30, 100)
        self.db.close()

    def load(self):
        print("Start Load Twitter...")
        shared_user_id = SharedInt(max_value=Config.num_users)
        loaders = []
        for _ in range(Config.num_loaders):
            conn = self.db.connect(init=True)
            loader = Loader(conn, shared_user_id)
            loaders.append(loader)
            loader.start()
        for loader in loaders:
            loader.join()
        self.db.close()
        print("Load Twitter Done")
        
    @classmethod
    def run(cls, database: DBManager, num_transactions=10000):
        cls.db = database
        Config.num_transactions = num_transactions
        twitter = Twitter()
        twitter.create_tables()
        twitter.load()
        print("Start run Twitter benchmark...")
        generator = TwitterGenerator()
        scheduler = Scheduler()
        term_manager = TerminalManager(
            Config, scheduler, generator, TwitterTerminal)
        app_manager = ApplicationManager(Config, cls.db, term_manager,TwitterApp)
        scheduler.set_app(app_manager)

        scheduler.start()
        term_manager.start()
        app_manager.start()

        scheduler.join()
        term_manager.join()
        app_manager.join()
        cls.db.close()
        print("Benchmark Done")
