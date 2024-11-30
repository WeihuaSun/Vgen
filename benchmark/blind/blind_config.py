from pathlib import Path


class BlindTable:
    table = "blind"
    columns = ["value"]


class Config:
    # data
    num_keys = 10000
    # load
    num_loaders = 16
    num_operations = 8  # operators per transaction

    weight_read = 50
    weight_update = 50

    # delay time
    delay_time = 3*1e9

    num_transactions = 10000
    num_sessions = 25
    num_terminals = 25
    num_monkeys = 4

    @classmethod
    def output_path(cls, session_id):
        type = 'wr'
        if cls.weight_read > cls.weight_update:
            type = 'rh'
        elif cls.weight_read < cls.weight_update:
            type = 'wh'
        return Path(f"./output/blindw_{type}_{cls.num_transactions}/{session_id}.log")


READ = "read"
UPDATE = "update"
