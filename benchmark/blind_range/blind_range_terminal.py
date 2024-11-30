import random


from benchmark.terminal import Terminal
from benchmark.blind_range.blind_range_config import *
from utils import RandomUtils


class BlindRangeTerminal(Terminal):
    def __init__(self, generator):
        super().__init__()
        self.generator = generator

    def generate(self):
        trans_due = self.trans_end + Config.delay_time
        chance = random.randint(1, 100)
        if chance <= Config.weight_read:
            self.ttype = READ
            self.record = self.generator.gen_read()
        else:
            self.ttype = UPDATE
            self.record = self.generator.gen_update()
        return trans_due

    def finish_time(self):
        return self.trans_end + 3*Config.delay_time


class BlindRangeGenerator:
    def __init__(self):
        self.rand_utils = RandomUtils()

    def gen_read(self):
        return PredRead(self.rand_utils)

    def gen_update(self):
        return PredUpdate(self.rand_utils)


class PredRead:
    def __init__(self, ru: RandomUtils):
        self.read_cols = []
        self.lefts = []
        self.rights = []
        counter = Config.num_operations
        left = ru.get_int(1, Config.num_keys//10)
        while counter > 0:
            counter -=1
            col = BlindRangeTable.columns[ru.get_int(
                0, len(BlindRangeTable.columns)-1)]
            left = min(left + ru.get_int(1,Config.num_keys//10), Config.num_keys)
            right = min(left + ru.get_int(1,100), Config.num_keys)
            self.read_cols.append(col)
            self.lefts.append(left)
            self.rights.append(right)


class PredUpdate:
    def __init__(self, ru: RandomUtils):
        self.read_cols = []
        self.lefts = []
        self.rights = []
        self.update_cols = []
        self.update_vals = []
        counter = Config.num_operations
        left = ru.get_int(1, Config.num_keys//10)
        while counter > 0:
            counter -=1
            col = BlindRangeTable.columns[ru.get_int(
                0, len(BlindRangeTable.columns)-1)]
            left = min(left + ru.get_int(1,Config.num_keys//10), Config.num_keys)
            right = min(left + ru.get_int(1,3), Config.num_keys)
            u_col =  BlindRangeTable.columns[ru.get_int(0, len(BlindRangeTable.columns)-1)]
            u_val = ru.get_int(1, Config.num_keys)
            self.read_cols.append(col)
            self.lefts.append(left)
            self.rights.append(right)
            self.update_cols.append(u_col)
            self.update_vals.append(u_val)
            
