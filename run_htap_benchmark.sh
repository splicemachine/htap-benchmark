#!/bin/bash
java -Xmx31G -cp `./classpath.sh bin` -Dlog4j.configuration=log4j.properties com.oltpbenchmark.DBWorkload $@

