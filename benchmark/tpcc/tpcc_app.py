import threading
import queue

from utils import *
from benchmark.application import Application
from benchmark.tpcc.tpcc_config import *



class SharedTables:
    def __init__(self):
        self.last_name2c_id_list = dict()
        self.c_id2o_id_list = dict()
        self.new_order = dict()  # [f,l]
        self.lock = threading.Lock()

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['lock']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.lock = threading.Lock()

    def get_c_id_list(self, key):
        with self.lock:
            return list(self.last_name2c_id_list[key])

    def insert_c_id(self, key, c_id):
        with self.lock:
            if key not in self.last_name2c_id_list.keys():
                self.last_name2c_id_list[key] = set()
            self.last_name2c_id_list[key].add(c_id)

    def get_o_id_list(self, key):
        with self.lock:
            return self.c_id2o_id_list[key]

    def insert_c2_o_id(self, key, o_id):
        with self.lock:
            if key not in self.c_id2o_id_list.keys():
                self.c_id2o_id_list[key] = set()
            self.c_id2o_id_list[key].add(o_id)

    def get_no_o_id(self, key):
        with self.lock:
            if key in self.new_order.keys():
                return self.new_order[key][0]

    def insert_new_order(self, key):
        with self.lock:
            if key not in self.new_order.keys():
                self.new_order[key] = [2101, 2101]
            self.new_order[key][1] += 1

    def delete_new_order(self, key):
        with self.lock:
            self.new_order[key][0] += 1


class TPCCApp(Application):
    def __init__(self, id, conn, queue: queue.PriorityQueue, lock: threading.Condition, terminal_manager, s_tables: SharedTables):
        super().__init__(id, conn, queue, lock, terminal_manager, Config)
        self.s_tables = s_tables

    def new_order(self, record):
        # input data
        w_id = record.w_id
        d_id = record.d_id
        d_w_id = record.d_w_id
        c_id = record.c_id
        c_w_id = record.c_w_id
        c_d_id = record.c_d_id
        ol_cnt = record.ol_cnt
        o_all_local = record.o_all_local
        ol_i_id_seq = record.ol_i_id_seq
        ol_supply_w_id_seq = record.ol_supply_w_id_seq
        ol_quantity_seq = record.ol_quantity_seq

        # Start transaction
        transaction = self.conn.begin()

        # Select the WAREHOUSE
        key = encode_key(w_id)
        value = self.conn.get(Warehouse.table, key)
        if value is False:
            return transaction
        if value is None:
            self.conn.abort()
            return transaction
        w_tax = decode_value(value)["w_tax"]/10000

        # Select the DISTRICT
        key = encode_key(d_w_id, d_id)
        value = self.conn.get(District.table, key)
        if value is False:
            return transaction
        if value is None:
            self.conn.abort()
            return transaction
        decoded_value = decode_value(value)
        d_tax = decoded_value["d_tax"]/10000
        o_id = decoded_value["d_next_o_id"]

        # Update the DISTRICT
        decoded_value["d_next_o_id"] = o_id + 1
        value = encode(decoded_value)
        if self.conn.set(District.table, key, value) is False:
            return transaction

        # Select the CUSTOMER
        key = encode_key(c_w_id, c_d_id, c_id)
        value = self.conn.get(Customer.table, key)
        if value is False:
            return transaction
        if value is None:
            self.conn.abort()
            return transaction
        decoded_value = decode_value(value)
        c_discount = decoded_value["c_discount"]/10000
        c_last = decoded_value["c_last"]
        c_credit = decoded_value["c_credit"]

        # Insert into ORDER
        o_entry_d = make_timestamp()
        key = encode_key(c_w_id, c_d_id, o_id)
        value = encode_value(Order.columns, c_id,
                             o_entry_d, None, ol_cnt, o_all_local)
        if self.conn.insert(Order.table, key, value) is False:
            return transaction
        t_key = encode_key(c_w_id, c_d_id, c_id)
        self.s_tables.insert_c2_o_id(t_key, o_id)

        # Insert into NEW_ORDER
        key = encode_key(c_w_id, c_d_id, o_id)
        value = encode_value(NewOrder.columns)
        if self.conn.insert(NewOrder.table, key, value) is False:
            return transaction
        t_key = encode_key(c_w_id, c_d_id)
        self.s_tables.insert_new_order(t_key)

        # Insert into ORDER_LINE and update STOCK
        distinct_key = []
        distinct_ol_w = []
        distinct_ol_i = []
        total_amount = 0
        for i in range(ol_cnt):
            ol_i_id = ol_i_id_seq[i]
            ol_supply_w_id = ol_supply_w_id_seq[i]
            ol_quantity = ol_quantity_seq[i]
            ol_number = i+1
            # Select the ITEM.
            key = encode_key(ol_i_id)
            value = self.conn.get(Item.table, key)
            if value is False:
                return transaction
            if value is None:
                self.conn.abort()
                return transaction
            decoded_value = decode_value(value)
            i_price = decoded_value["i_price"]
            i_name = decoded_value["i_name"]
            i_data = decoded_value["i_data"]

            # Select the STOCK
            key = encode_key(ol_supply_w_id, ol_i_id)
            if key in distinct_key:
                print(key)
                index = distinct_key.index(key)
                print(f"ol_supply_w_id1 {ol_supply_w_id} ---ol_supply_w_id2 {distinct_ol_w[index]}")
                print(f"ol_i_id1 {ol_i_id} ---ol_i_id2 {distinct_ol_i[index]}")
                print(ol_supply_w_id_seq)
                print(ol_i_id_seq)
            value = self.conn.get(Stock.table, key)
            if value is False:
                return transaction
            if value is None:
                self.conn.abort()
                return transaction
            decoded_value = decode_value(value)
            s_quantity = decoded_value["s_quantity"]
            s_data = decoded_value["s_data"]
            s_dist = decoded_value["s_dist_"+record.d_id_str]

            # Update the STOCK
            s_ytd = decoded_value["s_ytd"] + ol_quantity
            s_order_cnt = decoded_value["s_order_cnt"] + 1
            if s_quantity >= ol_quantity + 10:
                s_quantity -= ol_quantity
            else:
                s_quantity += (91 - ol_quantity)
            decoded_value["s_quantity"] = s_quantity
            decoded_value["s_ytd"] = s_ytd
            decoded_value["s_order_cnt"] = s_order_cnt
            value = encode(decoded_value)
            if self.conn.set(Stock.table, key, value) is False:
                return transaction
            ol_amount = ol_quantity * i_price
            total_amount += ol_amount
            # Insert into ORDER_LINE
            key = encode_key(w_id, d_id, o_id, ol_number)
            value = encode_value(OrderLine.columns, ol_i_id, ol_supply_w_id,
                                 o_entry_d, ol_quantity, ol_amount, s_dist)
            if self.conn.insert(OrderLine.table, key, value) is False:
                return transaction
        total_amount = total_amount*(1-c_discount)*(1+w_tax+d_tax)
        # transaction end
        self.conn.commit()
        return transaction

    def payment(self, record):
        # input data
        w_id = record.w_id
        d_id = record.d_id

        c_id = record.c_id
        c_last = record.c_last

        c_d_id = record.c_d_id
        c_w_id = record.c_w_id

        h_id = record.h_id
        h_amount = record.h_amount

        # Start transaction
        transaction = self.conn.begin()

        # Select the WAREHOUSE.
        key = encode_key(w_id)
        value = self.conn.get(Warehouse.table, key)
        if value is False:
            return transaction
        if value is None:
            self.conn.abort()
            return transaction
        decoded_value = decode_value(value)
        w_name = decoded_value["w_name"]
        w_street_1 = decoded_value["w_street_1"]
        w_street_2 = decoded_value["w_street_2"]
        w_city = decoded_value["w_city"]
        w_state = decoded_value["w_state"]
        w_zip = decoded_value["w_zip"]

        # Update the WAREHOUSE
        decoded_value["w_ytd"] = decoded_value["w_ytd"]+h_amount
        value = encode(decoded_value)
        if self.conn.set(Warehouse.table, key, value) is False:
            return transaction

        # Select the DISTRICT.
        key = encode_key(w_id, d_id)
        value = self.conn.get(District.table, key)
        if value is False:
            return transaction
        if value is None:
            self.conn.abort()
            return transaction
        decoded_value = decode_value(value)
        d_name = decoded_value["d_name"]
        d_street_1 = decoded_value["d_street_1"]
        d_street_2 = decoded_value["d_street_2"]
        d_city = decoded_value["d_city"]
        d_state = decoded_value["d_state"]
        d_zip = decoded_value["d_zip"]

        # Update the DISTRICT.
        decoded_value["d_ytd"] = decoded_value["d_ytd"]+h_amount
        value = encode(decoded_value)
        if self.conn.set(District.table, key, value) is False:
            return transaction

        # C_LAST(60%)
        if c_id is None:
            t_key = encode_key(c_w_id, c_d_id, c_last)
            c_id_list = self.s_tables.get_c_id_list(t_key)
            t_c_id = c_id_list[len(c_id_list) // 2]
            for c_id in c_id_list:
                if c_id == t_c_id:
                    continue
                key = encode_key(c_w_id, c_d_id, c_id)
                value = self.conn.get(Customer.table, key)
                if value is False:
                    return transaction
                if value is None:
                    self.conn.abort()
                    return transaction
            c_id = t_c_id
        # Select the CUSTOMER
        key = encode_key(c_w_id, c_d_id, c_id)
        value = self.conn.get(Customer.table, key)
        if value is False:
            return transaction
        if value is None:
            self.conn.abort()
            return transaction
        decoded_value = decode_value(value)
        c_first = decoded_value["c_first"]
        c_middle = decoded_value["c_middle"]
        c_last = decoded_value["c_last"]
        c_street_1 = decoded_value["c_street_1"]
        c_street_2 = decoded_value["c_street_2"]
        c_city = decoded_value["c_city"]
        c_state = decoded_value["c_state"]
        c_zip = decoded_value["c_zip"]
        c_phone = decoded_value["c_phone"]
        c_since = decoded_value["c_since"]
        c_credit = decoded_value["c_credit"]
        c_credit_lim = decoded_value["c_credit_lim"]
        c_discount = decoded_value["c_discount"]
        c_balance = decoded_value["c_balance"]

        # Update the CUSTOMER
        decoded_value["c_balance"] = decoded_value["c_balance"]-h_amount
        decoded_value["c_ytd_payment"] = decoded_value["c_ytd_payment"]+h_amount
        decoded_value["c_payment_cnt"] = decoded_value["c_payment_cnt"]+1
        if c_credit == "BC":
            c_data = decoded_value["c_data"]
            sb_data = "C_ID={} C_D_ID={} C_W_ID={} D_ID={} W_ID={} H_AMOUNT={:.2f}   ".format(
                c_id, c_d_id, c_w_id, d_id, w_id, h_amount)
            sb_data += c_data
            if len(sb_data) > 500:
                sb_data = sb_data[:500]
            decoded_value["c_data"] = sb_data
        value = encode(decoded_value)
        if self.conn.set(Customer.table, key, value) is False:
            return transaction

        # Insert into HISTORY
        h_date = make_timestamp()
        h_data = w_name+"    "+d_name
        key = encode_key(h_id)
        value = encode_value(History.columns, c_id, c_d_id, c_w_id, d_id,
                             w_id, h_date, h_amount, h_data)
        if self.conn.insert(History.table, key, value) is False:
            return transaction
        # transaction end
        self.conn.commit()
        return transaction

    def order_status(self, record):
        # input data
        w_id = record.w_id
        d_id = record.d_id
        c_w_id = record.c_w_id
        c_d_id = record.c_d_id
        c_id = record.c_id
        c_last = record.c_last

        # Start transaction
        transaction = self.conn.begin()

        # C_LAST(60%)
        if c_id is None:
            t_key = encode_key(c_w_id, c_d_id, c_last)
            c_id_list = self.s_tables.get_c_id_list(t_key)
            t_c_id = c_id_list[len(c_id_list) // 2]
            for c_id in c_id_list:
                if c_id == t_c_id:
                    continue
                key = encode_key(c_w_id, c_d_id, c_id)
                value = self.conn.get(Customer.table, key)
                if value is False:
                    return transaction
                if value is None:
                    self.conn.abort()
                    return transaction
            c_id = t_c_id

        # Select the CUSTOMER
        key = encode_key(c_w_id, c_d_id, c_id)
        value = self.conn.get(Customer.table, key)
        if value is False:
            return transaction
        if value is None:
            self.conn.abort()
            return transaction
        decoded_value = decode_value(value)
        c_first = decoded_value["c_first"]
        c_middle = decoded_value["c_middle"]
        c_last = decoded_value["c_last"]
        c_balance = decoded_value["c_balance"]

        # Select the latest ORDER for the CUSTOMER
        key = encode_key(c_w_id, c_d_id, c_id)
        o_id_list = self.s_tables.get_o_id_list(key)
        o_id = max(o_id_list)

        key = encode_key(c_w_id, c_d_id, o_id)
        value = self.conn.get(Order.table, key)
        if value is False:
            return transaction
        if value is None:
            self.conn.abort()
            return transaction
        decoded_value = decode_value(value)
        o_entry_d = decoded_value["o_entry_d"]
        o_carrier_id = decoded_value["o_carrier_id"]
        o_ol_cnt = decoded_value["o_ol_cnt"]

        # Select ORDER_LINEs for the ORDER
        for ol_number in range(1, o_ol_cnt+1):
            key = encode_key(c_w_id, c_d_id, o_id, ol_number)
            value = self.conn.get(OrderLine.table, key)
            if value is False:
                return transaction
            if value is None:
                self.conn.abort()
                return transaction
            decoded_value = decode_value(value)
            ol_i_id = decoded_value["ol_i_id"]
            ol_supply_w_id = decoded_value["ol_supply_w_id"]
            ol_delivery_d = decoded_value["ol_delivery_d"]
            ol_quantity = decoded_value["ol_quantity"]
            ol_amount = decoded_value["ol_amount"]

        # transaction end
        self.conn.commit()
        return transaction

    def delivery(self, record):
        # input data
        w_id = record.w_id
        o_carrier_id = record.o_carrier_id
        ol_delivery_d = make_timestamp()

        # Start transaction
        transaction = self.conn.begin()

        for d_id in range(1, Config.districts_per_warehouse + 1):

            t_key = encode_key(w_id, d_id)
            no_o_id = self.s_tables.get_no_o_id(t_key)  # min no_o_id

            key = encode_key(w_id, d_id, no_o_id)
            # Select the oldest NEW_ORDER for the DISTRICT
            value = self.conn.get(NewOrder.table, key)
            if value is False:
                return transaction
            if value is None:
                self.conn.abort()
                return transaction
            # Delete from NEW_ORDER
            if self.conn.delete(NewOrder.table, key) is False:
                return transaction
            t_key = encode_key(w_id, d_id)
            self.s_tables.delete_new_order(t_key)
            # Select the ORDER
            key = encode_key(w_id, d_id, no_o_id)
            value = self.conn.get(Order.table, key)
            if value is False:
                return transaction
            if value is None:
                self.conn.abort()
                return transaction
            decoded_value = decode_value(value)
            o_c_id = decoded_value["o_c_id"]
            ol_cnt = decoded_value["o_ol_cnt"]

            # Update the ORDER
            decoded_value["o_carrier_id"] = o_carrier_id
            value = encode(decoded_value)
            if self.conn.set(Order.table, key, value) is False:
                return transaction

            # Select ORDER_LINEs and update them
            ol_total = 0
            for ol_number in range(1, ol_cnt+1):
                key = encode_key(w_id, d_id, no_o_id, ol_number)
                value = self.conn.get(OrderLine.table, key)
                if value is False:
                    return transaction
                if value is None:
                    self.conn.abort()
                    return transaction
                decoded_value = decode_value(value)
                decoded_value["order_lines"] = ol_delivery_d
                ol_total += decoded_value["ol_amount"]
                value = encode(decoded_value)
                if self.conn.set(OrderLine.table, key, value) is False:
                    return transaction

            # Update the CUSTOMER's balance and delivery count
            key = encode_key(w_id, d_id, o_c_id)
            value = self.conn.get(Customer.table, key)
            if value is False:
                return transaction
            if value is None:
                self.conn.abort()
                return transaction
            decoded_value = decode_value(value)
            decoded_value["c_balance"] += ol_total
            decoded_value["c_delivery_cnt"] += 1
            value = encode(decoded_value)
            if self.conn.set(Customer.table, key, value) is False:
                return transaction

        # Transaction end
        self.conn.commit()
        return transaction

    def stock_level(self, record):
        # input data
        w_id = record.w_id
        d_id = record.d_id
        threshold = record.threshold

        # Start transaction
        transaction = self.conn.begin()

        # Select the DISTRICT
        key = encode_key(w_id, d_id)
        value = self.conn.get(District.table, key)
        if value is False:
            return transaction
        if value is None:
            self.conn.abort()
            return transaction
        decoded_value = decode_value(value)
        d_next_o_id = decoded_value["d_next_o_id"]

        # Select the last 20 ORDERs for the DISTRICT
        order_ids = range(d_next_o_id - 20, d_next_o_id)
        low_stock_count = 0
        for o_id in order_ids:
            key = encode_key(w_id, d_id, o_id)
            value = self.conn.get(Order.table, key)
            if value is False:
                return transaction
            if value is None:
                self.conn.abort()
                return transaction
            decoded_value = decode_value(value)
            o_ol_cnt = decoded_value["o_ol_cnt"]
            for ol_number in range(1, o_ol_cnt+1):
                key = encode_key(w_id, d_id, o_id, ol_number)
                value = self.conn.get(OrderLine.table, key)
                if value is False:
                    return transaction
                if value is None:
                    self.conn.abort()
                    return transaction
                decoded_value = decode_value(value)
                ol_i_id = decoded_value["ol_i_id"]
                key = encode_key(w_id, ol_i_id)
                value = self.conn.get(Stock.table, key)
                if value is False:
                    return transaction
                if value is None:
                    self.conn.abort()
                    return transaction
                decoded_value = decode_value(value)
                s_quantity = decoded_value["s_quantity"]
                if s_quantity < threshold:
                    low_stock_count += 1
        # transaction end
        self.conn.commit()
        return transaction
