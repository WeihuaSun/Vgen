from pathlib import Path


class Warehouse:
    table = "t_warehouse"
    key = ["w_id"]
    columns = ["w_name", "w_street_1", "w_street_2",
               "w_city", "w_state", "w_zip", "w_tax", "w_ytd"]


class District:
    table = "t_district"
    key = ["d_w_id", "d_id"]
    columns = ["d_name", "d_street_1", "d_street_2",
               "d_city", "d_state", "d_zip", "d_tax", "d_ytd", "d_next_o_id"]


class Customer:
    table = "t_customer"
    key = ["c_w_id", "c_d_id", "c_id"]
    columns = ["c_first", "c_middle", "c_last", "c_street_1", "c_street_2", "c_city", "c_state", "c_zip",
               "c_phone", "c_since", "c_credit", "c_credit_lim", "c_discount", "c_balance", "c_ytd_payment", "c_payment_cnt", "c_delivery_cnt", "c_data"]


class History:
    table = "t_history"
    key = []
    columns = ["h_c_id", "h_c_d_id", "h_c_w_id", "h_d_id",
               "h_w_id", "h_date", "h_amount", "h_data"]


class NewOrder:
    table = "t_new_order"
    key = ["no_w_id", "no_d_id", "no_o_id"]
    columns = []


class Order:
    table = "t_order"
    key = ["o_w_id", "o_d_id", "o_id"]
    columns = ["o_c_id", "o_entry_d",
               "o_carrier_id", "o_ol_cnt", "o_all_local"]


class OrderLine:
    table = "t_order_line"
    key = ["ol_w_id", "ol_d_id", "ol_o_id", "ol_number"]
    columns = ["ol_i_id",
               "ol_supply_w_id", "ol_delivery_d", "ol_quantity", "ol_amount", "ol_dist_info"]


class Item:
    table = "t_item"
    key = ["i_id"]
    columns = ["i_im_id", "i_name", "i_price", "i_data"]


class Stock:
    table = "t_stock"
    key = ["s_w_id","s_i_id"]
    columns = ["s_quantity", "s_dist_01", "s_dist_02", "s_dist_03", "s_dist_04", "s_dist_05",
               "s_dist_06", "s_dist_07", "s_dist_08", "s_dist_09", "s_dist_10", "s_ytd", "s_order_cnt", "s_remote_cnt", "s_data"]


class Last2Cid:
    table = "last2cid"
    columns = ["cid"]


class Cid2Oid:
    table = "cid2oid"
    columns = ["oid"]


class Config:
    delay_time = 30*1e9

    num_warehouses = 1
    districts_per_warehouse = 10
    customers_per_district = 3000

    num_loaders = 16

    weight_new_order = 43
    weight_payment = 4
    weight_order_status = 4
    weight_delivery = 4
    weight_stock_level = 45

    num_terminals = num_warehouses*districts_per_warehouse
    num_sessions = 16
    num_transactions = 10000
    num_monkeys = 4
    
    @classmethod
    def output_path(cls, session_id):
        return Path(f"./output/tpcc_{cls.num_transactions}/{session_id}.log")


NEW_ORDER = "new_order"
PAYMENT = "payment"
ORDER_STATUS = "order_status"
DELIVERY = "delivery"
STOCK_LEVEL = "stock_level"
