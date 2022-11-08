#!/bin/bash

BIN_DIR="$( dirname "${BASH_SOURCE[0]}" )"
pushd "$BIN_DIR" > /dev/null

source ./config.sh

if [[ "${KERBEROS}" == "true" ]]; then
   url="jdbc:splice://$SPLICE_HOST:$SPLICE_PORT/splicedb;principal=${KERBEROS_PRINCIPAL};keytab=${KERBEROS_KEYTAB}"
else
   url="jdbc:splice://$SPLICE_HOST:$SPLICE_PORT/splicedb;user=$SPLICE_USERNAME;password=$SPLICE_PSWD"
fi

echo 'connecting to:' $url

if [[ $SOURCE == */database/* ]]; then
    echo "restore htap-$SCALE"
    cp template-restore.sql restore-htap.sql
    backupid=undefined
    if [[ $SCALE == "100" ]]; then
        backupid=2235534337
    elif [[ $SCALE == "250" ]]; then
        backupid=355638273
    elif [[ $SCALE == "1000" ]]; then
        backupid=1384221697
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

popd > /dev/null
echo 'done'
