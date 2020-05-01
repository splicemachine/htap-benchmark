#!/bin/bash

BIN_DIR="$( dirname "${BASH_SOURCE[0]}" )"
pushd "$BIN_DIR" > /dev/null

# Default values
IM=0	# Throughput Monitoring Interval in milliseconds
SW=300	# Sampling window

source ./config.sh

# Allow overriding parameters on the command line as KEY=VALUE
for param in "$@"
do
   eval $param
done

echo "execute htap-$SCALE benchmark, time: $TIME  tpcc: $CWORKERS  chbenchmark: $HWORKERS"
# Write config file (use Mac/Linux compatible 'sed -i')
cp template-config.xml config.xml
if [[ "${KERBEROS}" -eq "true" ]];
   then url="jdbc:splice://$SPLICE_HOST:$SPLICE_PORT/splicedb;principal=${KERBEROS_PRINCIPAL};keytab=${KERBEROS_KEYTAB}"
else
   url="jdbc:splice://$SPLICE_HOST:$SPLICE_PORT/splicedb;user=$SPLICE_USERNAME;password=$SPLICE_PSWD"
fi

sed -i.bak -e "s%###SPLICE_URL###%$url%g" config.xml
sed -i.bak -e "s%###SCALE###%$SCALE%g" config.xml
sed -i.bak -e "s%###TIME###%$TIME%g" config.xml
sed -i.bak -e "s%###WORKERS_TPCC###%$CWORKERS%g" config.xml
sed -i.bak -e "s%###WORKERS_TPCH###%$HWORKERS%g" config.xml
rm -rf config.xml.bak

cd ..

# Changing DDL or dialect files requires rebuild of oltpbenchmark
if [[ "$PREBUILT" != "true" ]]; then
  rm -rf build
  ant
fi

mkdir -p results
session=htap-${SCALE}_${CWORKERS}_${HWORKERS}
./oltpbenchmark -b 'tpcc,chbenchmark' -c quickstart/config.xml --execute=true -im $IM -s $SW -ss -o $session | tee results/$session.out

popd > /dev/null
echo 'done'

