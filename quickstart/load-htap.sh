#!/bin/bash

source config.sh

echo "load htap-$SCALE"

url="jdbc:splice://$SPLICE_HOST:$SPLICE_PORT/splicedb;user=$SPLICE_USERNAME;password=$SPLICE_PSWD"
echo 'connecting to:' $url

cp template-load.sql load-htap.sql
sed -i.bak "s%###SOURCE###%$SOURCE%g" load-htap.sql
rm -rf load-htap.sql.bak

$SPLICE_HOME/sqlshell.sh -U $url -f load-htap.sql

echo 'done'
