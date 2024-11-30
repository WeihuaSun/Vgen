#!/bin/bash

BLINDW_WH_SIZE=10000
BLINDW_RH_SIZE=10000
BLINDW_WR_SIZE="10000 20000 30000 40000 50000 60000 70000 80000 90000 100000"

TPC_C_SIZE=10000
C_TWITTER_SIZE=10000
BLINDW_PRED_SIZE=10000

python gen_history.py blindw-wh $BLINDW_WH_SIZE
python gen_history.py blindw-rh $BLINDW_RH_SIZE
for size in $BLINDW_WR_SIZE; do
    python gen_history.py blindw-wr $size
done

python gen_history.py tpc-c $TPC_C_SIZE
python gen_history.py c-twitter $C_TWITTER_SIZE
python gen_history.py blindw-pred $BLINDW_PRED_SIZE

