from pathlib import Path


class BlindRangeTable:
    table = "blind_range"
    columns = ["A1", "A2"]


class Config:
    num_keys = 10000
    num_loaders = 16
    num_operations = 4  
    weight_read = 50
    weight_update = 50
    delay_time = 0.5
    num_transactions = 10000
    num_sessions = 10
    num_terminals = 25
    num_monkeys = 4

    @classmethod
    def output_path(cls, session_id):
        return Path(f"./output/blindw_pred_{cls.num_transactions}/{session_id}.log")


READ = "read"
UPDATE = "update"
