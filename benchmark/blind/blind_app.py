import threading
import queue

from utils import *
from benchmark.application import Application
from benchmark.blind.blind_config import *


class BlindApp(Application):
    def __init__(self, id, conn, queue: queue.PriorityQueue, lock: threading.Condition, terminal_manager):
        super().__init__(id, conn, queue, lock, terminal_manager, Config)

    def read(self, record):
        read_keys = record.keys
        transaction = self.conn.begin()
        for read_key in read_keys:
            key = encode_key(read_key)
            value = self.conn.get(BlindTable.table, key)
            if value is False:
                return transaction
            if value is None:
                self.conn.abort()
                return transaction
        self.conn.commit()
        return transaction

    def update(self, record):
        update_keys = record.keys
        update_values = record.values
        transaction = self.conn.begin()
        for update_key, value in zip(update_keys, update_values):
            key = encode_key(update_key)
            if self.conn.set(BlindTable.table, key, value) is False:
                return transaction
        self.conn.commit()
        return transaction
