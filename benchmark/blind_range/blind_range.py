from database.database import DBManager
from benchmark.blind_range.blind_range_config import *
from benchmark.blind_range.blind_range_load import Loader
from benchmark.blind_range.blind_range_terminal import *
from benchmark.blind_range.blind_range_app import *
from utils import SharedInt
from benchmark.scheduler import Scheduler
from benchmark.terminal import TerminalManager
from benchmark.application import ApplicationManager
from utils import encode_key, clear_path, dump_transaction


class BlindRange:
    def __init__(self):
        pass

    def drop_tables(self):
        conn = self.db.connect()
        conn.drop_table(BlindRangeTable.table)
        self.db.close()

    def create_tables(self):
        self.drop_tables()
        conn = self.db.connect()
        create = f"""
        CREATE TABLE {BlindRangeTable.table} (
            key SERIAL PRIMARY KEY,
            A1 INTEGER,
            A2 INTEGER,
            TID INTEGER,
            OID INTEGER
        );
        """
        conn.execute(create)
        self.db.close()

    def load(self):
        print("Start Load Blind Range ...")
        shared_key = SharedInt(max_value=Config.num_keys)
        loaders = []
        transactions = []
        for _ in range(Config.num_loaders):
            conn = self.db.connect(init=False)
            transactions.append(conn.begin())
            loader = Loader(conn, shared_key)
            loaders.append(loader)
            loader.start()
        for loader in loaders:
            loader.join()
        self.db.close()
        path = Config.output_path(-1)
        clear_path(path)
        for trx in transactions:
            dump_transaction(trx, path)

        print("Load Blind Range Done")

    @classmethod
    def run(cls, database: DBManager, weight_read=50, wight_update=50, num_transactions=10000, num_operations=4, num_terminals=15):
        cls.db = database
        Config.weight_read = weight_read
        Config.weight_update = wight_update
        Config.num_transactions = num_transactions
        Config.num_operations = num_operations
        Config.num_terminals = num_terminals
        blind = BlindRange()
        blind.create_tables()
        blind.load()
        print("Start run Blind Range benchmark...")
        print(
            f"Read: {Config.weight_read} % - Update: {Config.weight_update} %")
        generator = BlindRangeGenerator()
        scheduler = Scheduler()
        term_manager = TerminalManager(
            Config, scheduler, generator, BlindRangeTerminal)
        app_manager = ApplicationManager(
            Config, cls.db, term_manager, BlindRangeApp)
        scheduler.set_app(app_manager)

        scheduler.start()
        term_manager.start()
        app_manager.start()

        scheduler.join()
        term_manager.join()
        app_manager.join()
        cls.db.close()
        print("Blind  Range Benchmark Done")
