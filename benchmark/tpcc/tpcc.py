from database.database import DBManager
from utils import SharedInt, RandomUtils
from benchmark.scheduler import Scheduler
from benchmark.terminal import TerminalManager
from benchmark.application import ApplicationManager
from benchmark.tpcc.tpcc_config import *
from benchmark.tpcc.tpcc_load import *
from benchmark.tpcc.tpcc_terminal import *
from benchmark.tpcc.tpcc_app import *
import time


class TPCC:
    def __init__(self):
        pass
    def drop_tables(self):
        conn = self.db.connect(init=True)
        conn.drop_table(Warehouse.table)
        conn.drop_table(District.table)
        conn.drop_table(Customer.table)
        conn.drop_table(History.table)
        conn.drop_table(NewOrder.table)
        conn.drop_table(Order.table)
        conn.drop_table(OrderLine.table)
        conn.drop_table(Item.table)
        conn.drop_table(Stock.table)
        self.db.close()

    def create_tables(self):
        self.drop_tables()
        conn = self.db.connect(init=True)
        conn.create_table(Warehouse.table, 5, 220)
        conn.create_table(District.table, 10, 250)
        conn.create_table(Customer.table, 15, 1000)
        conn.create_table(History.table, 5, 200)
        conn.create_table(NewOrder.table, 15, 20)
        conn.create_table(Order.table, 15, 120)
        conn.create_table(OrderLine.table, 20, 200)
        conn.create_table(Item.table, 10, 150)
        conn.create_table(Stock.table, 15, 600)
        conn = self.db.connect(init=True)

    def load(self):
        print("Start Load TPC-C...")
        start = time.time()
        print("load item")
        i_id_offset = SharedInt(0, 100)  # 100,000/1000 = 100
        item_loaders = []
        for _ in range(Config.num_loaders):
            conn = self.db.connect(init=True)
            item_loader = ItemLoader(conn, i_id_offset, self.rand_utils)
            item_loaders.append(item_loader)
            item_loader.start()
        for item_loader in item_loaders:
            item_loader.join()
        self.db.close()
        for w_id in range(1, Config.num_warehouses+1):
            conn = self.db.connect(init=True)
            print(f"load warehouse: w_id = {w_id}")
            load_warehouse(w_id, conn, self.rand_utils)
            print("load stock")
            s_i_id_offset = SharedInt(0, 100)  # 100,000/1000 = 100
            stock_loaders = []
            for _ in range(Config.num_loaders):
                conn = self.db.connect(init=True)
                stock_loader = StockLoader(
                    conn, w_id, s_i_id_offset, self.rand_utils)
                stock_loaders.append(stock_loader)
                stock_loader.start()
            for stock_loader in stock_loaders:
                stock_loader.join()
            print("load district")
            load_district(w_id, conn, self.rand_utils)
            self.db.close()
            print("load order")
            order_loaders = []
            for d_id in range(1, 11):
                conn = self.db.connect(init=True)
                order_loader = OrderLoader(
                    conn, w_id, d_id, self.s_h_id, self.rand_utils, self.s_tables)
                order_loaders.append(order_loader)
                order_loader.start()
            for order_loader in order_loaders:
                order_loader.join()
            self.db.close()
        print(f"Load TPC-C Done. Time cost {time.time()-start}")
    
    @classmethod
    def run(cls, database: DBManager,num_transactions = 10000):
        cls.db = database
        cls.rand_utils = RandomUtils()
        cls.s_tables = SharedTables()
        cls.s_h_id = SharedInt()
        Config.num_transactions = num_transactions
        tpcc =TPCC()
        tpcc.create_tables()
        tpcc.load()
        print("Start run TPCC benchmark...")
        start = time.time()
        generator = TPCCGenerator(cls.rand_utils, cls.s_h_id)
        scheduler = Scheduler()

        term_args = []
        for w_id in range(1, Config.num_warehouses+1):
            for d_id in range(1, Config.districts_per_warehouse+1):
                term_args.append([w_id, d_id])

        term_manager = TerminalManager(
            Config, scheduler, generator, TPCCTerminal, *term_args)
        app_manager = ApplicationManager(
            Config, cls.db, term_manager, TPCCApp, cls.s_tables)
        scheduler.set_app(app_manager)

        scheduler.start()
        term_manager.start()
        app_manager.start()

        scheduler.join()
        print("Scheduler done")
        term_manager.join()
        print("Terminals done")
        app_manager.join()
        cls.db.close()
        print(f"TPC-C Benchmark Done. Time cost {time.time()-start}")
