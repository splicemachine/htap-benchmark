#!/bin/bash
echo 'generate htap dataset in bzip2'

pushd ../../htap-benchmark

# generate tpcc file in blocks of 1000 warehouses - note that blocksize MUST be a multiple of worksize and worksize must be the scalefactor in config file
worksize=250
blocksize=1000
multiple=$((blocksize/worksize))
totalsize=10000
w_id=1
until [ $w_id -gt $totalsize ]
do
	echo generating warehouses $w_id to $((w_id+blocksize-1))
	directory=/tmp/tpcc-$(printf %05d $((w_id+blocksize-1)))
	mkdir $directory

	# loop through worksize
	incr=0
	until [ $incr -ge $multiple ]
	do
		./oltpbenchmark -b 'tpcc,chbenchmark' -c ~/dev/tko/htap_config_gen.xml --load=true -startid $((w_id+worksize*incr)) -filelocation $directory > $directory/log$((w_id+worksize*incr)).txt
		((incr++))
	done

	echo consolidating files
	pushd $directory
	filenames='customer district history new_order oorder order_line stock warehouse'
	for filename in $filenames
	do
		cat $filename-* > $filename.csv
		rm $filename-*
	done

	echo compressing
	bzip2 * &

	popd
	((w_id += blocksize))
done

popd

echo 'Done'
