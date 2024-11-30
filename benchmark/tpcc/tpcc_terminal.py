import random

from benchmark.terminal import Terminal
from utils import *
from benchmark.tpcc.tpcc_config import *


class TPCCGenerator:
    def __init__(self, rand_utils: RandomUtils, s_h_id: SharedInt):
        self.rand_utils = rand_utils
        self.s_h_id = s_h_id

    def gen_new_order(self, w_id):
        d_id = self.rand_utils.get_int(1, Config.districts_per_warehouse)
        c_id = self.rand_utils.get_c_id()
        ol_cnt = self.rand_utils.get_int(5, 15)
        trans_rbk = (self.rand_utils.get_int(1, 100) == 1)
        ol_i_id_seq = []
        ol_supply_w_id_seq = []
        ol_quantity_seq = []
        o_all_local = 1
        for i in range(ol_cnt):
            iid = self.rand_utils.get_i_id()
            while iid in ol_i_id_seq:  # !design for Cobra-newOrder
                iid = self.rand_utils.get_i_id()
            ol_i_id_seq.append(iid)
            ol_supply_w_id = w_id
            # # In 1% of order lines the supply warehouse is different from the terminal's home warehouse.
            # if self.rand_utils.get_int(1, 100) == 1 and Config.num_warehouses > 1:
            #     o_all_local = 0
            #     while ol_supply_w_id == w_id:
            #         ol_supply_w_id = self.rand_utils.get_int(
            #             1, Config.num_warehouses)
            ol_supply_w_id_seq.append(ol_supply_w_id)
            ol_quantity_seq.append(self.rand_utils.get_int(1, 10))
        if trans_rbk:
            ol_i_id_seq[-1] = 999999
        return NewOrder(w_id, d_id, c_id, ol_cnt, o_all_local, ol_i_id_seq, ol_supply_w_id_seq, ol_quantity_seq)

    def gen_payment(self, w_id):
        d_id = self.rand_utils.get_int(1, Config.districts_per_warehouse)
        # in 85% of cases (c_d_id, c_w_id) = (d_id, w_id) in 15% of cases they are randomly chosen.
        if self.rand_utils.get_int(1, 100) <= 85:
            c_d_id = d_id
            c_w_id = w_id
        else:
            c_d_id = self.rand_utils.get_int(1, 10)
            c_w_id = w_id
            if Config.num_warehouses > 1:
                while c_w_id == w_id:
                    c_w_id = self.rand_utils.get_int(1, Config.num_warehouses)
        # in 60% of cases customer is selected by last name,in 40% of cases by customer ID.
        if self.rand_utils.get_int(1, 100) <= 60:
            c_id = None
            c_last = self.rand_utils.get_c_last()
        else:
            c_id = self.rand_utils.get_c_id()
            c_last = None
        h_id = self.s_h_id.increment()
        h_amount = self.rand_utils.get_int(1, 5000)
        return Payment(w_id, d_id, c_id, c_last, c_d_id, c_w_id, h_id, h_amount)

    def gen_order_status(self, w_id):
        d_id = self.rand_utils.get_int(1, Config.districts_per_warehouse)
        # in 60% of cases customer is selected by last name,in 40% of cases by customer ID.
        if self.rand_utils.get_int(1, 100) <= 60:
            c_id = None
            c_last = self.rand_utils.get_c_last()
        else:
            c_id = self.rand_utils.get_c_id()
            c_last = None
        return OrderStatus(w_id, d_id, c_id, c_last)

    def gen_delivery(self, w_id):
        o_carrier_id = self.rand_utils.get_int(1, 10)
        return Delivery(w_id, o_carrier_id)

    def gen_stock_level(self, w_id, d_id):
        threshold = self.rand_utils.get_int(10, 20)
        return StockLevel(w_id, d_id, threshold)


class NewOrder:
    def __init__(self, w_id, d_id, c_id, ol_cnt, o_all_local, ol_i_id_seq, ol_supply_w_id_seq, ol_quantity_seq):
        self.w_id = w_id
        self.d_id = d_id
        self.d_w_id = w_id
        self.c_id = c_id
        self.c_d_id = d_id
        self.c_w_id = w_id
        self.ol_cnt = ol_cnt
        self.o_all_local = o_all_local
        self.d_id_str = str(d_id) if d_id == 10 else "0"+str(d_id)
        self.ol_i_id_seq = ol_i_id_seq
        self.ol_supply_w_id_seq = ol_supply_w_id_seq
        self.ol_quantity_seq = ol_quantity_seq


class Payment:
    def __init__(self, w_id, d_id, c_id, c_last, c_d_id, c_w_id, h_id, h_amount):
        self.w_id = w_id
        self.d_id = d_id
        self.c_id = c_id
        self.c_last = c_last
        self.c_d_id = c_d_id
        self.c_w_id = c_w_id
        self.h_id = h_id
        self.h_amount = h_amount


class OrderStatus:
    def __init__(self, w_id, d_id, c_id, c_last):
        self.w_id = w_id
        self.d_id = d_id
        self.c_w_id = w_id
        self.c_d_id = d_id
        self.c_id = c_id
        self.c_last = c_last


class Delivery:
    def __init__(self, w_id, o_carrier_id):
        self.w_id = w_id
        self.o_carrier_id = o_carrier_id


class StockLevel:
    def __init__(self, w_id, d_id, threshold):
        self.w_id = w_id
        self.d_id = d_id
        self.threshold = threshold


class TPCCTerminal(Terminal):
    generator: TPCCGenerator

    def __init__(self, generator, w_id, d_id):
        super().__init__()
        self.generator = generator
        self.w_id = w_id
        self.d_id = d_id
        self.next_think_time = 0

    def generate(self):

        chance = random.randint(1, 100)
        key_time = 0
        think_time = self.next_think_time
        if chance <= Config.weight_new_order:
            self.ttype = NEW_ORDER
            key_time = 18*1e9
            self.next_think_time = 12*1e9
            self.record = self.generator.gen_new_order(self.w_id)
            return self.trans_end + think_time+key_time
        chance -= Config.weight_new_order
        if chance <= Config.weight_payment:
            self.ttype = PAYMENT
            key_time = 3*1e9
            self.next_think_time = 12*1e9
            self.record = self.generator.gen_payment(self.w_id)
            return self.trans_end + think_time+key_time
        chance -= Config.weight_payment
        if chance <= Config.weight_order_status:
            self.ttype = ORDER_STATUS
            key_time = 2*1e9
            self.next_think_time = 10*1e9
            self.record = self.generator.gen_order_status(self.w_id)
            return self.trans_end + think_time+key_time
        chance -= Config.weight_order_status
        if chance <= Config.weight_delivery:
            self.ttype = DELIVERY
            key_time = 2*1e9
            self.next_think_time = 5*1e9
            self.record = self.generator.gen_delivery(self.w_id)
            return self.trans_end + think_time+key_time
        else:
            self.ttype = STOCK_LEVEL
            key_time = 2*1e9
            self.next_think_time = 5*1e9
            self.record = self.generator.gen_stock_level(self.w_id, self.d_id)
            return self.trans_end + think_time+key_time

    def finish_time(self):
        # assume this time is later than each transaction's due time
        return self.trans_end + 3*Config.delay_time
