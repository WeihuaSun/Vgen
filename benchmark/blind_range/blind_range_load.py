import threading
from utils import RandomUtils, SharedInt, encode_key, encode_value, set_bit_map_at
from benchmark.blind_range.blind_range_config import *


class Loader(threading.Thread):
    def __init__(self, conn, s_key: SharedInt):
        super().__init__()
        self.conn = conn
        self.ru = RandomUtils()
        self.s_key = s_key

    def load_blind(self, key):
        # insert = f"INSERT INTO {BlindRangeTable.table} (key, A1, A2,OID,TID) VALUES({key}, {self.ru.get_int(1,10000)},{self.ru.get_int(1,10000)},0,0);"
        # self.conn.execute(insert)
        self.conn.insert_multi(BlindRangeTable.table, ['key', 'A1', 'A2'],[key,self.ru.get_int(1,10000),self.ru.get_int(1,10000)])

    def run(self):
        while True:
            key = self.s_key.increment()
            if key == -1:
                break
            self.load_blind(key)
        self.conn.commit()
