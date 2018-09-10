#!/bin/bash
set -x
echo 'execute htap benchmark'

pushd ..

rm -R build
ant

./oltpbenchmark -b 'tpcc' -c config/htap_config_splicemachine.xml --execute=true -s 300 -ss -o htap1000_1000-0_5 >> ./results/htap1000_1000-0_5.out
./oltpbenchmark -b 'tpcc,chbenchmark' -c config/htap_config_splicemachine.xml --execute=true -s 300 -ss -o htap1000_1000-1_5 >> ./results/htap1000_1000-1_5.out
./oltpbenchmark -b 'tpcc,chbenchmark' -c config/htap_config_splicemachine_2.xml --execute=true -s 300 -ss -o htap1000_1000-2_5 >> ./results/htap1000_1000-2_5.out
./oltpbenchmark -b 'tpcc,chbenchmark' -c config/htap_config_splicemachine_3.xml --execute=true -s 300 -ss -o htap1000_1000-3_5 >> ./results/htap1000_1000-3_5.out
./oltpbenchmark -b 'tpcc,chbenchmark' -c config/htap_config_splicemachine_4.xml --execute=true -s 300 -ss -o htap1000_1000-4_5 >> ./results/htap1000_1000-4_5.out

popd

echo 'Done'
