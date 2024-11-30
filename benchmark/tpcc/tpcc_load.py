import threading
from benchmark.tpcc.tpcc_app import SharedTables
from utils import RandomUtils, SharedInt, encode_key, encode_value, make_timestamp
from benchmark.tpcc.tpcc_config import *


class ItemLoader(threading.Thread):
    def __init__(self, conn, i_id_offset: SharedInt, rand_utils: RandomUtils):
        super().__init__()
        self.conn = conn
        self.i_id_offset = i_id_offset
        self.rand_utils = rand_utils

    def load_item_batch(self, offset):
        self.conn.begin()
        start_i_id = offset*1000+1
        for i in range(1000):
            i_id = start_i_id + i
            key = encode_key(i_id)
            i_im_id = self.rand_utils.get_int(1, 100000)
            i_name = self.rand_utils.get_string(14, 24)
            i_price = self.rand_utils.get_int(
                100, 10000)  # /100-[1.00..100.00]
            # For 10% of the rows, selected at random , the string "ORIGINAL"
            # must be held by 8 consecutive characters starting at a random
            # position within I_DATA
            if self.rand_utils.get_int(1, 100) <= 10:
                len = self.rand_utils.get_int(26, 50)
                off = self.rand_utils.get_int(0, len-8)
                i_data = self.rand_utils.get_string(
                    off, off) + "ORIGINAL"+self.rand_utils.get_string(len-off-8, len-off-8)
            else:
                i_data = self.rand_utils.get_string(26, 50)
            value = encode_value(Item.columns, i_im_id,
                                 i_name, i_price, i_data)
            self.conn.insert(Item.table, key, value)
        self.conn.commit()

    def run(self):
        while True:
            offset = self.i_id_offset.increment()  # start from 0
            # print(f"offset{offset}")
            if offset == -1:
                break
            self.load_item_batch(offset-1)


def load_warehouse(w_id, conn, rand_utils):
    conn.begin()
    key = encode_key(w_id)
    w_name = rand_utils.get_string(6, 10)
    w_street_1 = rand_utils.get_string(10, 20)
    w_street_2 = rand_utils.get_string(10, 20)
    w_city = rand_utils.get_string(10, 20)
    w_state = rand_utils.get_string(2, 2)
    w_zip = rand_utils.get_num_string(4, 4)+"11111"
    w_tax = rand_utils.get_int(0, 2000)  # /10000
    w_ytd = 3000000  # need .00
    value = encode_value(Warehouse.columns, w_name, w_street_1, w_street_2,
                         w_city, w_state, w_zip, w_tax, w_ytd)
    conn.insert(Warehouse.table, key, value)
    conn.commit()


class StockLoader(threading.Thread):
    def __init__(self, conn, w_id, s_i_id_offset: SharedInt, rand_utils: RandomUtils):
        super().__init__()
        self.conn = conn
        self.w_id = w_id
        self.s_i_id_offset = s_i_id_offset
        self.rand_utils = rand_utils

    def load_stock_batch(self, offset):
        self.conn.begin()
        s_w_id = self.w_id
        start_s_i_id = offset*1000+1
        for i in range(1000):
            s_i_id = start_s_i_id+i
            key = encode_key(s_w_id, s_i_id)
            s_quantity = self.rand_utils.get_int(10, 100)
            s_dist = [self.rand_utils.get_string(24, 24) for _ in range(10)]
            s_ytd = 0
            s_order_cnt = 0
            s_remote_cnt = 0
            # For 10% of the rows, selected at random , the string "ORIGINAL"
            # must be held by 8 consecutive characters starting at a random
            # position within S_DATA
            if self.rand_utils.get_int(1, 100) <= 10:
                len = self.rand_utils.get_int(26, 50)
                off = self.rand_utils.get_int(0, len-8)
                s_data = self.rand_utils.get_string(
                    off, off) + "ORIGINAL"+self.rand_utils.get_string(len-off-8, len-off-8)
            else:
                s_data = self.rand_utils.get_string(26, 50)
            value = encode_value(
                Stock.columns, s_quantity, *s_dist, s_ytd, s_order_cnt, s_remote_cnt, s_data)
            self.conn.insert(Stock.table, key, value)

        self.conn.commit()

    def run(self):
        while True:
            offset = self.s_i_id_offset.increment()  # start from 0
            if offset == -1:
                break
            self.load_stock_batch(offset-1)


def load_district(w_id, conn, rand_utils):
    conn.begin()
    d_w_id = w_id
    for d_id in range(1, 11):
        key = encode_key(d_w_id, d_id)
        d_name = rand_utils.get_string(6, 10)
        d_street_1 = rand_utils.get_string(10, 20)
        d_street_2 = rand_utils.get_string(10, 20)
        d_city = rand_utils.get_string(10, 20)
        d_state = rand_utils.get_string(2, 2)
        d_zip = rand_utils.get_num_string(4, 4) + "11111"
        d_tax = rand_utils.get_int(0, 2000)  # /10000
        d_ytd = 30000  # need .00
        d_next_o_id = 3001
        value = encode_value(District.columns, d_name, d_street_1,
                             d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id)
        conn.insert(District.table, key, value)

    conn.commit()


class OrderLoader(threading.Thread):
    def __init__(self, conn, w_id, d_id, s_h_id: SharedInt, rand_utils: RandomUtils, s_tables: SharedTables):
        super().__init__()
        self.rand_utils = rand_utils
        self.conn = conn
        self.s_h_id = s_h_id
        self.w_id = w_id
        self.d_id = d_id
        self.s_tables = s_tables

    def load_customer(self):  # checked
        self.conn.begin()
        c_d_id = self.d_id
        c_w_id = self.w_id
        for c_id in range(1, 3001):
            if c_id % 1000 == 0:
                self.conn.commit()
                self.conn.begin()
            key = encode_key(c_w_id, c_d_id, c_id)
            c_first = self.rand_utils.get_string(8, 16)
            c_middle = "OE"
            if c_id <= 1000:
                c_last = self.rand_utils.get_c_last_u(c_id-1)
            else:
                c_last = self.rand_utils.get_c_last()
            c_street_1 = self.rand_utils.get_string(10, 20)
            c_street_2 = self.rand_utils.get_string(10, 20)
            c_city = self.rand_utils.get_string(10, 20)
            c_state = self.rand_utils.get_string(2, 2)
            c_zip = self.rand_utils.get_num_string(4, 4) + "11111"
            c_phone = self.rand_utils.get_num_string(16, 16)
            c_since = make_timestamp()
            c_credit = "BC" if self.rand_utils.get_int(1, 100) <= 10 else "GC"
            c_credit_lim = 50000
            c_discount = self.rand_utils.get_int(0, 5000)  # /10000
            c_balance = -10
            c_ytd_payment = 10
            c_payment_cnt = 1
            c_delivery_cnt = 0
            c_data = self.rand_utils.get_string(300, 500)
            value = encode_value(Customer.columns, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone,
                                 c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data)
            self.conn.insert(Customer.table, key, value)
            t_key = encode_key(c_w_id, c_d_id, c_last)
            self.s_tables.insert_c_id(t_key, c_id)

        self.conn.commit()

    def load_history(self):  # checked
        self.conn.begin()
        h_c_w_id = self.w_id
        h_w_id = self.w_id
        h_c_d_id = self.d_id
        h_d_id = self.d_id
        for c_id in range(1, 3001):
            if c_id % 1000 == 0:
                self.conn.commit()
                self.conn.begin()
            key = encode_key(self.s_h_id.increment())
            h_c_id = c_id
            h_date = make_timestamp()
            h_amount = 10
            h_data = self.rand_utils.get_string(12, 24)
            value = encode_value(History.columns, h_c_id, h_c_d_id, h_c_w_id,
                                 h_d_id, h_w_id, h_date, h_amount, h_data)
            self.conn.insert(History.table, key, value)
        self.conn.commit()

    def load_order(self):  # checked
        # print(f"Load order: w_id = {self.w_id}, d_id = {self.d_id}")
        self.conn.begin()
        o_w_id = self.w_id
        o_d_id = self.d_id
        o_c_id_seq = self.rand_utils.get_shuffled_integers(1, 3000)
        for o_id in range(1, 3001):
            if o_id % 1000 == 0:
                self.conn.commit()
                self.conn.begin()
            key = encode_key(o_w_id, o_d_id, o_id)
            o_c_id = o_c_id_seq[o_id-1]
            o_entry_d = make_timestamp()
            o_carrier_id = self.rand_utils.get_int(
                1, 10) if o_id < 2101 else None
            o_ol_cnt = self.rand_utils.get_int(5, 15)
            o_all_local = 1
            value = encode_value(Order.columns, o_c_id,
                                 o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)
            self.conn.insert(Order.table, key, value)
            self.load_order_line(o_id, o_ol_cnt, o_entry_d)
            t_key = encode_key(o_w_id, o_d_id, o_c_id)
            self.s_tables.insert_c2_o_id(t_key, o_id)
        self.conn.commit()

    def load_order_line(self, o_id, o_ol_cnt, o_entry_d):  # checked
        ol_o_id = o_id
        ol_d_id = self.d_id
        ol_w_id = self.w_id
        for ol_number in range(1, o_ol_cnt + 1):
            key = encode_key(ol_w_id, ol_d_id, ol_o_id, ol_number)
            ol_i_id = self.rand_utils.get_int(1, 100000)
            ol_supply_w_id = self.w_id
            ol_delivery_d = o_entry_d if o_id < 2101 else None
            ol_quantity = 5
            ol_amount = 0 if o_id < 2101 else self.rand_utils.get_int(
                1, 999999)  # need /100
            ol_dist_info = self.rand_utils.get_string(24, 24)
            value = encode_value(OrderLine.columns, ol_i_id, ol_supply_w_id,
                                 ol_delivery_d, ol_quantity, ol_amount, ol_dist_info)
            self.conn.insert(OrderLine.table, key, value)
            # self.conn.commit()
            # committed by load order

    def load_new_order(self):  # checked
        self.conn.begin()
        no_d_id = self.d_id
        no_w_id = self.w_id
        for o_id in range(2101, 3001):
            no_o_id = o_id
            key = encode_key(no_w_id, no_d_id, no_o_id)
            value = encode_value(NewOrder.columns)
            self.conn.insert(NewOrder.table, key, value)
            t_key = encode_key(no_w_id, no_d_id)
            self.s_tables.insert_new_order(t_key)
        self.conn.commit()

    def run(self):
        self.load_customer()
        self.load_history()
        self.load_order()
        self.load_new_order()
