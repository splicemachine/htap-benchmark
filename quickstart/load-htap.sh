#!/bin/bash

source config.sh

url="jdbc:splice://$SPLICE_HOST:$SPLICE_PORT/splicedb;user=$SPLICE_USERNAME;password=$SPLICE_PSWD"
echo 'connecting to:' $url

if [[ $SOURCE == */database/* ]]; then
    echo "restore htap-$SCALE"
    cp template-restore.sql restore-htap.sql
    backupid=undefined
    if [[ $SCALE == "1000" ]]; then
        backupid=2826810625
    elif [[ $SCALE == "10000" ]]; then
        backupid=39810049
    fi
    sed -i.bak "s%###SOURCE###%$SOURCE%g" restore-htap.sql
    sed -i.bak "s%###BACKUPID###%$backupid%g" restore-htap.sql
    rm -rf restore-htap.sql.bak
    $SPLICE_HOME/sqlshell.sh -U $url -f restore-htap.sql
else
    echo "load htap-$SCALE"
    cp template-load.sql load-htap.sql
    sed -i.bak "s%###SOURCE###%$SOURCE%g" load-htap.sql
    rm -rf load-htap.sql.bak
    $SPLICE_HOME/sqlshell.sh -U $url -f load-htap.sql
fi

echo 'done'