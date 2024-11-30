import threading
from utils import RandomUtils, SharedInt, encode_key, encode_value, set_bit_map_at
from benchmark.blind.blind_config import *


class Loader(threading.Thread):
    def __init__(self, conn, s_key: SharedInt):
        super().__init__()
        self.conn = conn
        self.ru = RandomUtils()
        self.s_key = s_key

    def load_blind(self, key):
        self.conn.begin()
        key = encode_key(key)
        value = self.ru.get_string(140, 140)

        self.conn.insert(BlindTable.table, key, value)
        self.conn.commit()

    def run(self):
        while True:
            key = self.s_key.increment()
            if key == -1:
                break
            self.load_blind(key)
