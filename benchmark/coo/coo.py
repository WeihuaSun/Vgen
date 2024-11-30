import queue
import threading
import os
import re
from pathlib import Path
import time

from database.database import DBManager
from utils import encode_key, clear_path, dump_transaction

case_root = "./benchmark/coo/cases/"
output = "./output/coo"
table = "t1"

def parse_select(sql):
    pattern = r'SELECT .*? WHERE k=(\d+);'
    match = re.search(pattern, sql, re.IGNORECASE)
    if match:
        k = int(match.group(1))
        return k
    return None  

def parse_select_predicate(sql):
    pattern = r'SELECT .*? WHERE v >= (\d+) AND v <= (\d+);'
    match = re.search(pattern, sql, re.IGNORECASE)
    if match:
        left = int(match.group(1))
        right = int(match.group(2))
        return left, right
    return  None, None

def parse_update(sql):
    pattern = r'UPDATE .*? SET v=(\d+) WHERE k=(\d+);'
    match = re.search(pattern, sql, re.IGNORECASE)
    if match:
        v = int(match.group(1))
        k = int(match.group(2))
        return k, str(v)
    return None, None  

def parse_update_predicate(sql):
    pattern = r'UPDATE .*? SET v=(\d+) WHERE v >= (\d+) AND v <= (\d+);'
    match = re.search(pattern, sql, re.IGNORECASE)
    if match:
        v = int(match.group(1))
        left = int(match.group(2))
        right = int(match.group(3))
        return str(v), left, right
    return None,None,None   


def parse_insert(sql):
    pattern = r'VALUES\s*\((\d+),\s*(\d+)\)'
    match = re.search(pattern, sql, re.IGNORECASE)
    if match:
        k = int(match.group(1))
        v = int(match.group(2))
        return k, str(v)
    return None  


def extract_number(stat):
    match = re.search(r'-(\d+)-', stat)
    if match:
        return match.group(1)
    return None


def extract_command(stat):
    match = re.search(r'-(?:[^-]*-)(.)', stat)
    if match:
        return match.group(1)
    return None

class Coo:
    def __init__(self, database: DBManager, statements, case_name) -> None:
        self.db = database
        self.case_name = case_name
        self.next_statement_to_execute = 2
        self.transactions = [0 for i in range(3)]
        self.num_statements = len(statements)
        self.insert_key = []
        self.statements = statements
        self.is_predicate = "pred" in case_name
        if self.is_predicate:
            self.create_tables_predicate()
            self.load_predicate()
        else:
            self.create_tables()
            self.load()
        self.conn1 = self.db.connect()
        self.conn2 = self.db.connect()
        self.conn3 = self.db.connect()
        self.queue1 = queue.Queue()
        self.queue2 = queue.Queue()
        self.lock = threading.Lock()

    def drop_tables(self):
        conn = self.db.connect()
        conn.drop_table(table)
        self.db.close()

    def create_tables(self):
        self.drop_tables()
        conn = self.db.connect()
        conn.create_table(table, 20, 20)
        self.db.close()
        
    def create_tables_predicate(self):
        self.drop_tables()
        self.drop_tables()
        conn = self.db.connect()
        create = f"""
        CREATE TABLE {table} (
            k INTEGER PRIMARY KEY,
            v INTEGER,
            TID INTEGER,
            OID INTEGER
        );
        """
        conn.execute(create)
        self.db.close()

    def load(self):
        conn = self.db.connect(init=True)
        conn.begin()
        for i in range(self.next_statement_to_execute, self.num_statements):
            stat = self.statements[i]
            if stat[0] == '0':
                self.next_statement_to_execute += 1
                command = stat[4].upper()
                if command == "I":
                    key, value = parse_insert(stat)
                    self.insert_key.append(key)
                    key = encode_key(key)
                    ret = conn.insert(table, key, value)
                    assert ret == True
                elif command == "C":
                    break
        conn.commit()
        self.db.close()
        
    def load_predicate(self):
        conn = self.db.connect()
        txn = conn.begin()
        for i in range(self.next_statement_to_execute, self.num_statements):
            stat = self.statements[i]
            if stat[0] == '0':
                self.next_statement_to_execute += 1
                command = stat[4].upper()
                if command == "I":
                    key, value = parse_insert(stat)
                    self.insert_key.append(key)
                    # insert = f"INSERT INTO {table} (k,v,OID,TID) VALUES({key},{value} ,0,0);"
                    # conn.execute(insert)
                    conn.insert_multi(table,['k','v'],[key,value])
                elif command == "C":
                    break
        conn.commit()
        output_dir = f"{output}/{self.case_name}/"
        path = Path(output_dir+"0.log")
        clear_path(path)
        dump_transaction(txn, path)
        #self.db.close()
        
    def dump(self):
        output_dir = f"{output}/{self.case_name}/"
        for i in range(3):
            path = Path(output_dir+f"{i+1}.log")
            txn = self.transactions[i]
            clear_path(path)
            dump_transaction(txn, path)

    def handle_statement(self, stat, conn):
        print(stat)
        command = extract_command(stat).upper()
        tid = int(extract_number(stat))
        if command == "B":
            self.transactions[tid-1] = conn.begin()
        elif command == "S":
            left, right = parse_select_predicate(stat)
            if left is not None:
                # left, right = parse_select_predicate(stat)
                value = conn.get_range(table,"v",left,right)
            else:
                key = parse_select(stat)
                key = encode_key(key)
                value = conn.get(table, key)
        elif command == "U":
            value, left, right = parse_update_predicate(stat)
            if left is not None:
                #value,left, right = parse_update_predicate(stat)
                ret = conn.update_range(table,"v",left,right,"v",value)
            else:
                key, value = parse_update(stat)
                if self.is_predicate:
                    conn.set_with_pred(table,'k',key,'v',value)
                else:
                    key = encode_key(key)
                    ret = conn.set(table, key, value)
        elif command == "C":
            conn.commit()
        elif command == "R":
            conn.abort()
    def worker(self, conn, task_queue):
        while True:
            try:
                stat = task_queue.get(timeout=2) 
                self.handle_statement(stat, conn)
                task_queue.task_done()
            except queue.Empty:
                break 
    def execute(self):
        thread1 = threading.Thread(target=self.worker, args=(
            self.conn1, self.queue1), name="Thread-1")
        thread2 = threading.Thread(target=self.worker, args=(
            self.conn2, self.queue2), name="Thread-2")
        thread1.start()
        thread2.start()
        while self.next_statement_to_execute < len(self.statements):
            with self.lock:
                stat = self.statements[self.next_statement_to_execute]
                current_tid = int(extract_number(stat))
                if current_tid == 1:
                    self.queue1.put(stat)
                elif current_tid == 2:
                    self.queue2.put(stat)
                self.next_statement_to_execute += 1
            time.sleep(0.1) 
        self.queue1.join()
        self.queue2.join()
        thread1.join()
        thread2.join()
        conn = self.conn3
        self.transactions[-1] = conn.begin()
        for k in self.insert_key:
            if self.is_predicate:
                conn.get_with_pred(table,'k',k)
            else:
                key = encode_key(k)
                value = conn.get(table, key)
        conn.commit()
        self.db.close()
        self.dump()


def run_all_cases(database: DBManager):
    for root, dirs, files in os.walk(case_root):
        for file in files:
            file_name = os.path.splitext(file)[0]
            print(f"case: {file_name}")
            file_path = os.path.join(root, file)
            with open(file_path, 'r', encoding='utf-8') as f:
                statements = f.readlines()
                coo = Coo(database, statements, file_name)
                coo.execute()
        print('-' * 40)