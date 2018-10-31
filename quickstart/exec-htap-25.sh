#!/bin/bash
echo 'execute htap-25 benchmark'

pushd ..

# always rebuild - changing DDL or dialect files requires rebuild of oltpbenchmark
rm -R build
ant

./oltpbenchmark -b 'tpcc,chbenchmark' -c config/htap-25_config_splicemachine_4.xml --execute=true -s 300 -ss -o htap-25_4
./oltpbenchmark -b 'tpcc,chbenchmark' -c config/htap-25_config_splicemachine_2.xml --execute=true -s 300 -ss -o htap-25_2
./oltpbenchmark -b 'tpcc,chbenchmark' -c config/htap-25_config_splicemachine_1.xml --execute=true -s 300 -ss -o htap-25_1
./oltpbenchmark -b 'tpcc'             -c config/htap-25_config_splicemachine_1.xml --execute=true -s 300 -ss -o htap-25_0

popd

echo 'Done'
