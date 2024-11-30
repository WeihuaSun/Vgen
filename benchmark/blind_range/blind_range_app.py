import threading
import queue

from utils import *
from benchmark.application import Application
from benchmark.blind_range.blind_range_config import *


class BlindRangeApp(Application):
    def __init__(self, id, conn, queue: queue.PriorityQueue, lock: threading.Condition, terminal_manager):
        super().__init__(id, conn, queue, lock, terminal_manager, Config)

    def read(self, record):
        read_cols = record.read_cols
        lefts = record.lefts
        rights = record.rights
        transaction = self.conn.begin()
        for i in range(Config.num_operations):
            read_col = read_cols[i]
            left = lefts[i]
            right = rights[i]
            values = self.conn.get_range(
                BlindRangeTable.table, read_col, left, right)
            if values is False:
                return transaction
            if values is None:
                self.conn.abort()
                return transaction
        self.conn.commit()
        return transaction

    def update(self, record):
        read_cols = record.read_cols
        lefts = record.lefts
        rights = record.rights
        update_cols = record.update_cols
        update_vals = record.update_vals
        transaction = self.conn.begin()
        for i in range(Config.num_operations):
            read_col = read_cols[i]
            left = lefts[i]
            right = rights[i]
            update_col = update_cols[i]
            update_val = update_vals[i]
            values = self.conn.update_range(BlindRangeTable.table, read_col, left, right, update_col, update_val)
            if values is False:
                return transaction
            if values is None:
                self.conn.abort()
                return transaction
        self.conn.commit()
        return transaction
