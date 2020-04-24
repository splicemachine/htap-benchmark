#!/bin/bash

ABSPATH=$(dirname $0)

pushd ${ABSPATH}

source ./config.sh

#override parameters if passed
if [[ "$#" -ne "0" ]]; then
   if [[ "$#" -eq "5" ]]; then
        SPLICE_HOST=$1
        SCALE=$2
        CWORKERS=$3
        HWORKERS=$4
        TIME=$5
   else
      echo "When specifying override parameters, you must specify all of them: SPLCE_HOST SCALE CWORKERS HWORKERS TIME"
      exit 1
   fi
fi


echo "execute htap-$SCALE benchmark, time: $TIME  tpcc: $CWORKERS  chbenchmark: $HWORKERS"

# Write config file (use Mac/Linux compatible 'sed -i')
cp template-config.xml config.xml
sed -i.bak -e "s%###SPLICE_URL###%jdbc:splice://$SPLICE_HOST:$SPLICE_PORT/splicedb%g" config.xml
sed -i.bak -e "s%###SCALE###%$SCALE%g" config.xml
sed -i.bak -e "s%###TIME###%$TIME%g" config.xml
sed -i.bak -e "s%###WORKERS_TPCC###%$CWORKERS%g" config.xml
sed -i.bak -e "s%###WORKERS_TPCH###%$HWORKERS%g" config.xml
rm -rf config.xml.bak

echo "update config.xml complete."

pushd ..

mkdir -p results

session=htap-${SCALE}_${CWORKERS}_${HWORKERS}

echo ${session}
./oltpbenchmark -b 'tpcc,chbenchmark' -c quickstart/config.xml --execute=true -im 10000 -s 300 -ss -o $session | tee results/$session.out

popd
popd

echo 'done'
