# change this variable to point to sqlshell.sh location
SPLICE_HOME=/usr/bin

# change database connection parameters
SPLICE_HOST='localhost'
SPLICE_PORT=1527
SPLICE_USERNAME=splice
SPLICE_PSWD=admin

# choose the data set scale factor: 2, 25, 250, 1000, 10k
SCALE=25

# choose data source: S3, localFS/HDFS (make sure data is present in the file system)
IMPORT_S3='s3a://splice-benchmark-data/flat/HTAP'
IMPORT_FS='/data/htap'
SOURCE=$IMPORT_FS/htap-$SCALE

# benchmark parameters
TIME=30
CWORKERS=$SCALE
HWORKERS=4

