#!/bin/bash
echo 'load htap-25'

# change this variable if you installed Splice Machine in a different location
SPLICE_HOME=/usr/local/splicemachine

SPLICE_HOST='localhost'
SPLICE_PORT='1527'
SPLICE_USERNAME=${1:-splice}
SPLICE_PSWD=${2:-admin}
DATA_SET=${3:-htap-25}

echo 'using credentials:' $SPLICE_USERNAME $SPLICE_PSWD 'for data set:' $DATA_SET

$SPLICE_HOME/bin/sqlshell.sh -f load-$DATA_SET.sql

echo 'Done'
