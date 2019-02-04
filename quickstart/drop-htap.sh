#!/bin/bash

source config.sh

echo 'drop htap'

url="jdbc:splice://$SPLICE_HOST:$SPLICE_PORT/splicedb;user=$SPLICE_USERNAME;password=$SPLICE_PSWD"
echo 'connecting to:' $url

$SPLICE_HOME/sqlshell.sh -U $url -f drop-htap.sql

echo 'done'
