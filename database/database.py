import time
import psycopg2
import mysql.connector

from config import *
from utils import *
from database.operator import *
from database.transaction import Transaction


class DBManager:
    def __init__(self, config):
        self.config = config
        self.connect_pool = []
        self.tid_counter = SharedInt()
        self.oid_counter = SharedInt()
        if config == PostgreSQLConfig:
            self.db_type = "pg"
            self.connector = psycopg2.connect
            self.is_level = PostgreSQLConfig.is_level
        elif config == MySQLConfig:
            self.db_type = "mysql"
            self.connector = mysql.connector.connect
            self.is_level = MySQLConfig.is_level
        else:
            self.db_type = "unset"
            print("Unexpected DB!")

    def connect(self, init=False):
        try:
            conn = self.connector(
                user=self.config.user,
                password=self.config.password,
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                # options="-c client_encoding=UTF8"
            )
            conn.autocommit = False
            cursor = conn.cursor()
            cursor.execute(self.config.set_isolation(self.is_level))
            conn.commit()
            cursor.close()
            connection = Connection(
                conn, self.tid_counter, self.oid_counter, init)
            self.connect_pool.append(connection)
            return connection
        except Exception as error:
            print(f"Error connecting to {self.config.name}: {error}")

    def close(self):
        while self.connect_pool:
            conn = self.connect_pool.pop()
            conn.close()


class Connection:
    def __init__(self, conn, tid_counter: SharedInt, oid_counter: SharedInt, init=False):
        self.conn = conn
        self.tid_counter = tid_counter
        self.oid_counter = oid_counter
        self.cursor = self.conn.cursor()
        self.init = init
        self.transaction = None

    def close(self):
        self.cursor.close()
        self.conn.close()

    def create_table(self, table, key_len, value_len):
        self.cursor.execute(
            f"""CREATE TABLE {table}(
                k VARCHAR({key_len}) PRIMARY KEY,
                v VARCHAR({value_len}));
            """)
        self.conn.commit()

    def execute(self, sql):
        self.cursor.execute(sql)
        self.conn.commit()

    def drop_table(self, table):
        self.cursor.execute(f"drop table if EXISTS {table};")
        self.conn.commit()

    def begin(self):
        if self.init:
            self.cursor.execute("BEGIN;")
        else:
            oid = self.oid_counter.increment()
            start = time.time_ns()
            time.sleep(0.001)
            self.cursor.execute("BEGIN;")
            time.sleep(0.001)
            end = time.time_ns()
            begin = Begin(oid, start, end)
            self.transaction = Transaction(self.tid_counter.increment())
            self.transaction.set_start(start)
            self.transaction.add(begin)
        return self.transaction

    def commit(self):
        try:
            if self.init:
                self.conn.commit()
            else:
                oid = self.oid_counter.increment()
                start = time.time_ns()
                time.sleep(0.001)
                self.conn.commit()
                time.sleep(0.001)
                end = time.time_ns()
                commit = Commit(oid, start, end)
                self.transaction.set_end(end)
                self.transaction.add(commit)
            return True
        except Exception as e:
            self.abort()
            return False

    def abort(self):
        try:
            if self.init:
                self.conn.rollback()
            else:
                oid = self.oid_counter.increment()
                start = time.time_ns()
                time.sleep(0.001)
                self.conn.rollback()
                time.sleep(0.001)
                end = time.time_ns()
                abort = Abort(oid, start, end)
                self.transaction.set_end(end)
                self.transaction.add(abort)
            return True
        except:
            print("rollback failed!")
            return False

    # predicate key: k_v,column
    def insert_multi(self, table, cols, values):
        try:
            if self.init:
                oid = 0
            else:
                oid = self.oid_counter.increment()
            sql = f"INSERT INTO {table} ( {','.join(cols)}, TID , OID ) VALUES ( {','.join([ str(v) for v in values])},{self.transaction.tid}, {oid} ) ;"
            start = time.time_ns()
            time.sleep(0.001)
            self.cursor.execute(sql)
            time.sleep(0.001)
            end = time.time_ns()
            for col, value in zip(cols, values):
                insert = Write(oid, start, end, values[0], col, value)
                self.transaction.add(insert)
            return True
        except Exception as e:
            self.abort()
            return False

    def insert(self, table, key, value):
        try:
            if self.init:
                oid = 0
                tid = 0
            else:
                oid = self.oid_counter.increment()
                tid = self.transaction.tid
            packed_value = pack_value(value, tid, oid)
            packed_key = pack_key(table, key)
            sql = f"INSERT INTO {table} (k, v) VALUES ('{key}','{packed_value}') ;"
            start = time.time_ns()
            time.sleep(0.001)
            self.cursor.execute(sql)
            if not self.init:
                time.sleep(0.001)
                end = time.time_ns()
                insert = Write(oid, start, end, packed_key)
                self.transaction.add(insert)
            return True
        except Exception as e:
            self.abort()
            return False

    def delete(self, table, key):
        try:
            oid = self.oid_counter.increment()
            sql = f"DELETE FROM {table} WHERE k = '{key}' ;"
            start = time.time_ns()
            time.sleep(0.001)
            self.cursor.execute(sql)
            time.sleep(0.001)
            end = time.time_ns()
            packed_key = pack_key(table, key)
            delete = Write(oid, start, end, packed_key)
            self.transaction.add(delete)
            return True
        except Exception as e:
            # print(f"Delete error:{e}")
            self.abort()
            # print(sql)
            return False

    def get(self, table, key):
        try:
            oid = self.oid_counter.increment()
            sql = f"SELECT v FROM {table} WHERE k = '{key}';"
            start = time.time_ns()
            time.sleep(0.001)
            self.cursor.execute(sql)
            time.sleep(0.001)
            end = time.time_ns()
            rows = self.cursor.fetchone()
            if rows is None or (len(rows) == 0):
                return None
            packed_value = rows[0]
            value, from_tid, from_oid = unpack_value(packed_value)
            packed_key = pack_key(table, key)
            get = Read(oid, start, end, packed_key, from_tid, from_oid)
            self.transaction.add(get)
            return value
        except Exception as e:
            self.abort()
            return False

    def get_with_pred(self, table, read_col, read_val):
        try:
            oid = self.oid_counter.increment()
            sql = f"SELECT * FROM {table} WHERE {read_col} = '{read_val}';"
            start = time.time_ns()
            time.sleep(0.001)
            self.cursor.execute(sql)
            time.sleep(0.001)
            end = time.time_ns()
            rows = self.cursor.fetchone()
            if rows is None or (len(rows) == 0):
                return None
            key = rows[0]
            from_tid = rows[-2]
            from_oid = rows[-1]
            get = Read(oid, start, end, key, from_tid, from_oid)
            self.transaction.add(get)
            return rows
        except Exception as e:
            self.abort()
            return False

    def set(self, table, key, value):
        try:
            if self.init:
                oid = 0
                tid = 0
            else:
                oid = self.oid_counter.increment()
                tid = self.transaction.get_id()
            packed_key = pack_key(table, key)
            packed_value = pack_value(value, tid, oid)
            sql = f"UPDATE {table} SET v = '{packed_value}' WHERE k = '{key}' ;"
            start = time.time_ns()
            time.sleep(0.001)
            self.cursor.execute(sql)
            time.sleep(0.001)
            end = time.time_ns()
            set = Write(oid, start, end, packed_key)
            self.transaction.add(set)
            return True
        except Exception as e:
            self.abort()
            return False

    def set_with_pred(self, table, read_col, read_val, update_col, update_val):
        try:
            if self.init:
                oid = 0
                tid = 0
            else:
                oid = self.oid_counter.increment()
                tid = self.transaction.get_id()
            sql = f"UPDATE {table} SET {update_col} = {update_val}, OID = {oid}, TID = {tid} WHERE {read_col} = {read_val};"
            start = time.time_ns()
            time.sleep(0.001)
            self.cursor.execute(sql)
            time.sleep(0.001)
            end = time.time_ns()
            set = Write(oid, start, end, read_val, update_col, update_val)
            self.transaction.add(set)
            return True
        except Exception as e:
            self.abort()
            return False

    def get_range(self, table, read_col, left, right):
        try:
            oid = self.oid_counter.increment()
            sql = f"SELECT * FROM {table} WHERE {read_col} <= {right} AND {read_col} >= {left};"
            print(sql)
            start = time.time_ns()
            time.sleep(0.001)
            self.cursor.execute(sql)
            time.sleep(0.001)
            end = time.time_ns()
            rows = self.cursor.fetchall()
            # if rows is None or (len(rows) == 0):
            #     return None
            if rows is None:
                return None
            keys = []
            from_tid_list = []
            from_oid_list = []
            for row in rows:
                key = row[0]
                from_tid = row[-2]
                from_oid = row[-1]
                get = Read(oid, start, end, key, from_tid, from_oid)
                self.transaction.add(get)
                keys.append(key)
                from_tid_list.append(from_tid)
                from_oid_list.append(from_oid)
            pred = PredicateRead(oid, start, end, read_col,
                                 left, right, keys, from_tid_list, from_oid_list)
            self.transaction.add(pred)
            return rows
        except Exception as e:
            self.abort()
            return False

    def update_range(self, table, read_col, left, right, update_col, update_val):
        try:
            
            oid = self.oid_counter.increment()
            sql = f"UPDATE {table} SET {update_col} = {update_val}, OID = {oid}, TID = {self.transaction.tid} WHERE {read_col} >= {left} AND {read_col} <= {right} RETURNING *;"
            start = time.time_ns()
            time.sleep(0.001)
            self.cursor.execute(sql)
            time.sleep(0.001)
            end = time.time_ns()
            rows = self.cursor.fetchall()
            if rows is None:
                return None
            keys = []
            from_tid_list = []
            from_oid_list = []
            for row in rows:
                key = row[0]
                from_tid = row[-2]
                from_oid = row[-1]
                get = Write(oid, start, end, key, update_col, update_val)
                self.transaction.add(get)
                keys.append(key)
                from_tid_list.append(from_tid)
                from_oid_list.append(from_oid)
            pred = PredicateRead(oid, start, end, read_col,
                                 left, right, keys, from_tid_list, from_oid_list)
            self.transaction.add(pred)
            return rows
        except Exception as e:
            try:
                oid = self.oid_counter.increment()
                tid = self.transaction.tid
                select_sql = f"SELECT * FROM {table} WHERE {read_col} >= {left} AND {read_col} <= {right} FOR UPDATE;"
                self.cursor.execute(select_sql)
                rows = self.cursor.fetchall()
                if not rows:
                    return None
                update_sql = f"UPDATE {table} SET {update_col} = {update_val}, OID = {oid}, TID = {tid} WHERE {read_col} >= {left} AND {read_col} <= {right};"
                start = time.time_ns()
                time.sleep(0.001)
                self.cursor.execute(update_sql)
                time.sleep(0.001)
                end = time.time_ns()
                keys = []
                from_tid_list = []
                from_oid_list = []
                for row in rows:
                    key = row[0]
                    from_tid = row[-2]
                    from_oid = row[-1]
                    get = Write(oid, start, end, key, update_col, update_val)
                    self.transaction.add(get)
                    keys.append(key)
                    from_tid_list.append(from_tid)
                    from_oid_list.append(from_oid)

                pred = PredicateRead(
                    oid, start, end, read_col, left, right, keys, from_tid_list, from_oid_list)
                self.transaction.add(pred)
                return rows
            except:
                self.abort()
                return False
