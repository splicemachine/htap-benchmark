#!/bin/bash
set -x
echo 'drop htap1000'

SPLICE_HOST='localhost'
SPLICE_PORT='1527'
SPLICE_USERNAME=${1:-splice}
SPLICE_PSWD=${2:-admin}
DATA_SET=${3:-htap1000}

SQLSHELL_PATH=/usr/bin/sqlshell

echo 'using credentials:' $SPLICE_USERNAME $SPLICE_PSWD 'for data set:' $DATA_SET

$SQLSHELL_PATH/sqlshell.sh -h $SPLICE_HOST -p $SPLICE_PORT -u $SPLICE_USERNAME -s $SPLICE_PSWD -f drop-$DATA_SET.sql

echo 'Done'
