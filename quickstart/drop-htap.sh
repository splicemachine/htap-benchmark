#!/bin/bash
set -x
echo 'drop htap'

# change this variable if you installed Splice Machine in a different location
SPLICE_HOME=/usr/local

SPLICE_HOST='localhost'
SPLICE_PORT='1527'
SPLICE_USERNAME=${1:-splice}
SPLICE_PSWD=${2:-admin}

echo 'using credentials:' $SPLICE_USERNAME $SPLICE_PSWD

$SPLICE_HOME/bin/sqlshell.sh -f drop-htap.sql

echo 'Done'
