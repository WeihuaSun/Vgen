import hashlib
from datetime import datetime
import numpy as np
import threading
import pickle
import base64
import base64
import random
import string
import time
import pickle
import json


class RandomUtils:
    def __init__(self):
        random.seed(time.time_ns())
        self.c_last_tokens = ["BAR", "OUGHT", "ABLE", "PRI",
                              "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"]
        self.nURandCLast = self.get_int(0, 255)
        self.nURandCC_ID = self.get_int(0, 1023)
        self.nURandCI_ID = self.get_int(0, 8191)

    def get_string(self, min_length=5, max_length=10):
        char_set = string.ascii_letters + string.digits
        length = random.randint(min_length, max_length)
        return ''.join(random.choice(char_set)
                       for _ in range(length))

    def get_num_string(self, min_length, max_length):
        char_set = string.digits
        length = random.randint(min_length, max_length)
        return ''.join(random.choice(char_set)
                       for _ in range(length))

    def get_int(self, a, b):
        return random.randint(a, b)

    def get_bool(self):
        return random.choice([True, False])

    def get_c_last_u(self, num):
        ret = ""
        for i in range(3):
            ret += self.c_last_tokens[num % 10]
            num //= 10
        return ret

    def get_c_last(self):
        num = (((self.get_int(0, 255) | self.get_int(
            0, 999)) + self.nURandCLast) % 1000)
        return self.get_c_last_u(int(num))

    def get_c_id(self):
        part_1 = self.get_int(0, 1023)
        part_2 = self.get_int(1, 3000)
        return int(((part_1 | part_2) + self.nURandCC_ID) % 3000 + 1)

    def get_i_id(self):
        part_1 = self.get_int(0, 8191)
        part_2 = self.get_int(1, 100000)
        return int(((part_1 | part_2) + self.nURandCI_ID) % 100000 + 1)

    def get_shuffled_integers(self, start, end):
        return random.sample(range(start, end + 1), end - start + 1)


def encode_value(columns, *args):
    assert len(columns) == len(args)
    values = {}
    for i, arg in enumerate(args):
        values[columns[i]] = arg
    return encode(values)

def decode_value(encoded_value):
    values = decode(encoded_value)
    return values


def encode_key(*args):
    assert len(args) > 0
    if len(args) == 1:
        return encode(args[0])
    keys = []
    for arg in args:
        keys.append(arg)
    return encode(keys)


def decode_key(encoded_key):
    keys = decode(encoded_key)
    return keys


def encode(t) -> str:
    return json.dumps(t)
    return base64.b64encode(pickle.dumps(t)).decode('utf-8')


def decode(s):
    return json.loads(s)
    return pickle.loads(base64.b64decode(s))


def set_bit_map_at(b, p):
    b[p // 8] |= (1 << (p % 8))


def clear_bit_map_at(b, p):
    b[p // 8] &= ~(1 << (p % 8))


def get_bit_map_at(b, p):
    return (b[p // 8] & (1 << (p % 8))) != 0


class SharedInt:
    def __init__(self, initial_value=0, max_value=None):
        self.value = initial_value
        self.max_value = max_value
        self.lock = threading.Lock()

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['lock']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.lock = threading.Lock()

    def increment(self):
        with self.lock:
            if self.max_value is not None and self.value == self.max_value:
                return -1
            self.value += 1
            return self.value

    def get_value(self):
        with self.lock:
            return self.value


def pack_value(value, tid, oid):
    return value+"|"+str(tid)+"|"+str(oid)


def unpack_value(packed_value: str):
    rets = packed_value.split("|")
    return rets[0], int(rets[1]), int(rets[2])


def pack_key(table, encoded_key):
    return encode((table, encoded_key))


def random_bool():
    return random.choice([True, False])


class ZipfRangeGenerator:
    def __init__(self, a, min_val, max_val):
        self.a = a
        self.min_val = min_val
        self.max_val = max_val
        self.range_length = max_val - min_val + 1

    def next_value(self):
        zipf_sample = np.random.zipf(self.a)
        mapped_sample = (zipf_sample - 1) % self.range_length + self.min_val
        return mapped_sample


def make_timestamp():
    return datetime.now().strftime("%d-%m-%Y")


def dump_transaction(transaction, path):

    data = transaction.encode()
    with open(path, "ab") as f:
        f.write(data)


def clear_path(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()


def hash_string_to_bytes(s: str):
    hash_object = hashlib.sha256(s.encode())
    hash_bytes = hash_object.digest()[:8]
    return hash_bytes


def str_to_long(s) -> int:
    if isinstance(s, int):
        return s
    hash_object = hashlib.sha256(s.encode())
    hex_dig = hash_object.hexdigest()
    return int(hex_dig, 16) & ((1 << 64) - 1)

