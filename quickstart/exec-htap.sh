#!/bin/bash

source config.sh

echo "execute htap-$SCALE benchmark, time: $TIME  tpcc: $CWORKERS  chbenchmark: $HWORKERS"

# Write config file (use Mac/Linux compatible 'sed -i')
cp template-config.xml config.xml
sed -i.bak -e "s%###SPLICE_URL###%jdbc:splice://$SPLICE_HOST:$SPLICE_PORT/splicedb%g" config.xml
sed -i.bak -e "s%###SCALE###%$SCALE%g" config.xml
sed -i.bak -e "s%###TIME###%$TIME%g" config.xml
sed -i.bak -e "s%###WORKERS_TPCC###%$CWORKERS%g" config.xml
sed -i.bak -e "s%###WORKERS_TPCH###%$HWORKERS%g" config.xml
rm -rf config.xml.bak

pushd ..

# always rebuild - changing DDL or dialect files requires rebuild of oltpbenchmark
rm -rf build
ant

mkdir -p results

session=htap-${SCALE}_${CWORKERS}_${HWORKERS}
./oltpbenchmark -b 'tpcc,chbenchmark' -c quickstart/config.xml --execute=true -s 300 -ss -o $session | tee results/$session.out

popd

echo 'done'
