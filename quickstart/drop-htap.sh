#!/bin/bash

BIN_DIR="$( dirname "${BASH_SOURCE[0]}" )"
pushd "$BIN_DIR" > /dev/null

source ./config.sh

echo 'drop htap'

url="jdbc:splice://$SPLICE_HOST:$SPLICE_PORT/splicedb;user=$SPLICE_USERNAME;password=$SPLICE_PSWD"
echo 'connecting to:' $url

$SPLICE_HOME/sqlshell.sh -U $url -f drop-htap.sql

popd > /dev/null
echo 'done'
