import struct

class Transaction:
    def __init__(self, tid) -> None:
        self.tid = tid
        self.operators = []
        self.start = -1
        self.end = -1

    def add(self, operator):
        self.operators.append(operator)

    def set_start(self, start):
        self.start = start

    def set_end(self, end):
        self.end = end

    def get_id(self):
        return self.tid

    def encode(self):
        ret = b'T'
        ret += struct.pack("<I", self.tid)
        ret += struct.pack("<Q", self.start)
        ret += struct.pack("<Q", self.end)
        for op in self.operators:
            ret += op.encode()
        return ret
