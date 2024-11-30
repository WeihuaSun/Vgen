import random


from benchmark.terminal import Terminal
from benchmark.blind.blind_config import *
from utils import RandomUtils


class BlindTerminal(Terminal):
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


class BlindGenerator:
    def __init__(self):
        self.rand_utils = RandomUtils()

    def gen_read(self):
        return Read(self.rand_utils)

    def gen_update(self):
        return Update(self.rand_utils)


class Read:
    def __init__(self, ru: RandomUtils):
        self.keys = []
        counter = Config.num_operations
        while counter > 0:
            key = ru.get_int(1, Config.num_keys)
            if key not in self.keys:
                counter -= 1
                self.keys.append(key)


class Update:
    def __init__(self, ru: RandomUtils):
        self.keys = []
        self.values = []
        counter = Config.num_operations
        while counter > 0:
            key = ru.get_int(1, Config.num_keys)
            if key not in self.keys:
                counter -= 1
                self.keys.append(key)
                self.values.append(ru.get_string(140, 140))
