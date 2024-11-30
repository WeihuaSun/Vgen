import argparse

import config
from database.database import DBManager
from benchmark.blind.blind import Blind
from benchmark.blind_range.blind_range import BlindRange
from benchmark.tpcc.tpcc import TPCC
from benchmark.twitter.twitter import Twitter
from benchmark.coo.coo import run_all_cases


def main():
    db = DBManager(config.PostgreSQLConfig)
    BlindRange.run(db, 50, 50, 10000)
    db = DBManager(config.PostgreSQLConfig)
    Blind.run(db, 20, 80, 10000)
    db = DBManager(config.MySQLConfig)
    run_all_cases(db)
    parser = argparse.ArgumentParser()
    parser.add_argument('workload', type=str, choices=[
                        'blindw-wr', 'blindw-wh', 'blindw-rh', 'tpc-c', 'c-twitter', 'blindw-pred', 'coo'])
    parser.add_argument('size', type=int, default=10000)
    args = parser.parse_args()
    if args.workload == 'blindw-wr':
        db = DBManager(config.PostgreSQLConfig)
        Blind.run(db, 50, 50, args.size)
    elif args.workload == 'blindw-wh':
        db = DBManager(config.PostgreSQLConfig)
        Blind.run(db, 20, 80, args.size)
    elif args.workload == 'blindw-rh':
        db = DBManager(config.PostgreSQLConfig)
        Blind.run(db, 80, 20, args.size)
    elif args.workload == 'tpc-c':
        db = DBManager(config.PostgreSQLConfig)
        TPCC.run(db, args.size)
    elif args.workload == 'c-twitter':
        db = DBManager(config.PostgreSQLConfig)
        Twitter.run(db, args.size)
    elif args.workload == 'blindw-pred':
        db = DBManager(config.PostgreSQLConfig)
        BlindRange.run(db, 50, 50, args.size)
    elif args.workload == "coo":
        db = DBManager(config.MySQLConfig)
        run_all_cases(db)


if __name__ == '__main__':
    main()
