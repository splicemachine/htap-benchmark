Quick Start: running oltpbench for Splice

oltpbench executable: 

Currently the modified source code and configuration files supporting Splice Machine is in the repository https://github.com/splicemachine/htap-benchmark. The build requires Java 8. Included is an ant build script that will build the oltpbench executable. 

Scripts for loading and executing htap1000: 

The host information needs to be edited in the following files: 

•	drop-htap.sh and load-htap.sh: SPLICE_HOST variable and SQLSHELL_PATH variable.

•	config/htap_config_splicemachine_&lt;N&gt;.xml: for files 0-4 the database url needs to be modified to the correct host name. 

The following scripts are included with this quick start: 

•	reload-htap.sh <password> – this script reloads the data from s3 for the 1000 warehouse benchmark. Requires the host and password of the splice user for the cluster. This script will create a user htap1000 with password htapuser. This user and schema is used by the subsequent benchmark runs.

•	exec-htap.sh – this script builds and runs the chbenchmark for 1000 warehouses and 1000 tpc-c workers for 0, 1, 2, 3, and 4 tpc-h workers for five minute runs. 

Note: to change the number of minutes, change the <time> value (seconds) in each of the htap_config_splicemachine.xml files (currently set to 300). 

