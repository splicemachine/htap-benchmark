# OLTPBench

[![Build Status](https://travis-ci.org/oltpbenchmark/oltpbench.png)](https://travis-ci.org/oltpbenchmark/oltpbench)

Benchmarking is incredibly useful, yet endlessly painful. This benchmark suite is the result of a group of
Phd/post-docs/professors getting together and combining their workloads/frameworks/experiences/efforts. We hope this
will save other people's time, and will provide an extensible platform, that can be grown in an open-source fashion. 

OLTPBenchmark is a multi-threaded load generator. The framework is designed to be able to produce variable rate,
variable mixture load against any JDBC-enabled relational database. The framework also provides data collection
features, e.g., per-transaction-type latency and throughput logs.

Together with the framework we provide the following OLTP/Web benchmarks:
  * [TPC-C](http://www.tpc.org/tpcc/)
  * Wikipedia
  * Synthetic Resource Stresser 
  * Twitter
  * Epinions.com
  * [TATP](http://tatpbenchmark.sourceforge.net/)
  * [AuctionMark](http://hstore.cs.brown.edu/projects/auctionmark/)
  * SEATS
  * [YCSB](https://github.com/brianfrankcooper/YCSB)
  * [JPAB](http://www.jpab.org) (Hibernate)
  * [CH-benCHmark](http://www-db.in.tum.de/research/projects/CHbenCHmark/?lang=en)
  * [Voter](https://github.com/VoltDB/voltdb/tree/master/examples/voter) (Japanese "American Idol")
  * [SIBench](http://sydney.edu.au/engineering/it/~fekete/teaching/serializableSI-Fekete.pdf) (Snapshot Isolation)
  * [SmallBank](http://ses.library.usyd.edu.au/bitstream/2123/5353/1/michael-cahill-2009-thesis.pdf)
  * [LinkBench](http://people.cs.uchicago.edu/~tga/pubs/sigmod-linkbench-2013.pdf)

This framework is design to allow easy extension, we provide stub code that a contributor can use to include a new
benchmark, leveraging all the system features (logging, controlled speed, controlled mixture, etc.)

## Publications

If you are using this framework for your papers or for your work, please cite the paper:

[OLTP-Bench: An extensible testbed for benchmarking relational databases](http://www.vldb.org/pvldb/vol7/p277-difallah.pdf) D. E. Difallah, A. Pavlo, C. Curino, and P. Cudre-Mauroux. In VLDB 2014.

Also, let us know so we can add you to our [list of publications](http://oltpbenchmark.com/wiki/index.php?title=Publications_Using_OLTPBenchmark).

Please visit the project homepage for anything other than source code: <http://oltpbenchmark.com>

## Dependencies

+ Java (1.8)

## Support for Splice Machine on CH-benCHmark

This fork includes a [Quick Start](https://github.com/splicemachine/htap-benchmark/wiki) to get you up and running the CH-benCHmark with Splice Machine on OLTP-Bench. Enjoy! 

## Running HTAP Using Docker

You can run HTAP using a docker image as well.

The following command will restore the HTAP database and then it will run the benchmark

```
docker run -t -v `pwd`:/logs splicemachine/benchmark_htap:0.0.1 -j "jdbc:splice://localhost:1527/splicedb" -a restore
```

The following command will remove the HTAP database:

```
docker run -t -v `pwd`:/logs splicemachine/benchmark_htap:0.0.1 -j "jdbc:splice://localhost:1527/splicedb" -a destroy
```

If you prefer not to use the restored database, you can load it from flat files:

```
docker run -t -v `pwd`:/logs splicemachine/benchmark_htap:0.0.1 -j "jdbc:splice://localhost:1527/splicedb" -a create
```

The parameters that can be used are:

```
	-a, --action false		The action you want to preform.  Valid values: create, destroy, restore, none.  It defaults to none. 

	-e, --execute true		Execute the benchmark workload.  Valid values true / false.  It defaults to true. 

	-j, --jdbc_url 			JDBC Url for the splice machine datbase.  It defaults to jdbc:splice://localhost:1527/splicedb 

	-u, --user 			Splice Machine database user.  It defaults to splice 

	-p, --password 			Splice Machine password.  It defaults to admin 

	-n, --schemaName 		Htap Schema Name.  It defaults to htap 

	-r, --schemaUser 		Htap Schema User.  It defaults to htap 

	-o, --schemaUserPassword 	Htap Schema User password.  It defaults to htapuser 

	-d, --restoreDirectory 		Restore directory if action is restore.  Defaults to s3a://splice-benchmark-data/database/HTAP/100 

	-b, --backupId 			Backup id of the restored schema.  Defaults for scale 100 to 2235534337, scale 1000 to 1384221697 and scale 10000 to 39810049 

	-x, --restoreSouceSchema 	Schema of the restored database.  Defaults to htap 

	-y, --dataDirectory 		If creating a database, the location of the import files.  Defaults to s3a://splice-benchmark-data/flat/HTAP/htap-100 

	-m, --work_time 3600		The work time.  It defaults to 3600. 

	-w, --weights 			Comma delimited list of weights.  It defaults to "45,43,4,4,4". 

	-s, --sample true		Sampling window.  Numeric value.  It defaults to 300. 

	-i, --interval true		Throughput Monitoring Interval in milliseconds.  It defaults to 10000. 

	-l, --scale 100			Scale.  It defaults to 100. 

	-t, --tpch_sessions 1400	TPCH Sessions  It defaults to 4. 

	-c, --tpch_sessions 1400	TPCC Sessions  It defaults to 100. 

```

