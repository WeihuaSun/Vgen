import struct

from utils import str_to_long


class Operator:
    def __init__(self, operator_id, start, end):
        self.operator_id = operator_id
        self.start = start
        self.end = end

    def encode(self):
        raise NotImplementedError


class Read(Operator):
    def __init__(self, operator_id, start, end, key, from_tid, from_oid):
        super().__init__(operator_id, start, end)
        self.key = key
        self.from_tid = from_tid
        self.from_oid = from_oid

    def encode(self):
        op_type = b'R'
        op_id = struct.pack('<I', self.operator_id)
        start = struct.pack('<Q', self.start)
        end = struct.pack('<Q', self.end)
        key = struct.pack('<Q', str_to_long(self.key))
        from_tid = struct.pack('<I', self.from_tid)
        from_oid = struct.pack('<I', self.from_oid)
        ret = op_type + op_id + start + end + key+from_tid+from_oid
        return ret


class PredicateRead(Operator):
    def __init__(self, operator_id, start, end, read_col, left, right, keys, from_tid_list, from_oid_list):
        super().__init__(operator_id, start, end)
        self.keys = keys
        self.read_col = read_col
        self.left = left
        self.right = right
        self.from_tid_list = from_tid_list
        self.from_oid_list = from_oid_list

    def encode(self):
        ret = b'P'
        ret += struct.pack('<I', self.operator_id)
        ret += struct.pack('<Q', self.start)
        ret += struct.pack('<Q', self.end)
        ret += struct.pack('<Q', str_to_long(self.read_col))
        ret += struct.pack('<I', self.left)
        ret += struct.pack('<I', self.right)
        ret += struct.pack('<I', len(self.keys))
        for i in range(len(self.keys)):
            ret += struct.pack('<Q', self.keys[i])
        for i in range(len(self.keys)):
            ret += struct.pack('<I', self.from_tid_list[i])
        for i in range(len(self.keys)):
            ret += struct.pack('<I', self.from_tid_list[i])
        return ret


class Write(Operator):
    def __init__(self, operator_id, start, end, key, update_col="v", update_val=0):
        super().__init__(operator_id, start, end)
        self.key = key
        self.update_col = update_col
        self.update_val = update_val
    def encode(self):
        op_type = b'W'
        op_id = struct.pack('<I', self.operator_id)
        start = struct.pack('<Q', self.start)
        end = struct.pack('<Q', self.end)
        key = struct.pack('<Q', str_to_long(self.key))
        update_col = struct.pack('<Q', str_to_long(self.update_col))
        update_val = struct.pack('<I', int(self.update_val))
        ret = op_type + op_id + start + end + key+update_col+update_val
        return ret


class Begin(Operator):
    def __init__(self, operator_id, start, end):
        super().__init__(operator_id, start, end)

    def encode(self):
        op_type = b'S'
        op_id = struct.pack('<I', self.operator_id)
        start = struct.pack('<Q', self.start)
        end = struct.pack('<Q', self.end)
        ret = op_type + op_id + start + end
        return ret


class Commit(Operator):
    def __init__(self, operator_id, start, end):
        super().__init__(operator_id, start, end)

    def encode(self):
        op_type = b'C'
        op_id = struct.pack('<I', self.operator_id)
        start = struct.pack('<Q', self.start)
        end = struct.pack('<Q', self.end)
        ret = op_type + op_id + start + end
        return ret


class Abort(Operator):
    def __init__(self, operator_id, start, end):
        super().__init__(operator_id, start, end)

    def encode(self):
        op_type = b'A'
        op_id = struct.pack('<I', self.operator_id)
        start = struct.pack('<Q', self.start)
        end = struct.pack('<Q', self.end)
        ret = op_type + op_id + start + end
        return ret

