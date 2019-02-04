#!/bin/bash

source config.sh

echo "load htap-$SCALE"

url="jdbc:splice://$SPLICE_HOST:$SPLICE_PORT/splicedb;user=$SPLICE_USERNAME;password=$SPLICE_PSWD"
echo 'connecting to:' $url

cp template-load.sql load.sql
sed -i.bak "s%###SOURCE###%$SOURCE%g" load.sql
rm -rf load.sql.bak

$SPLICE_HOME/sqlshell.sh -U $url -f load.sql

echo 'done'
