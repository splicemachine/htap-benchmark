#!/bin/bash
set -x

# reload htap1000 using my credentials 

SPLICE_PSWD=${1:-admin}
DATA_SET=${2:-htap1000}

./drop-htap.sh splice $SPLICE_PSWD $DATA_SET

./load-htap.sh splice $SPLICE_PSWD $DATA_SET

