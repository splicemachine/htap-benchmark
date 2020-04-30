# change this variable to point to sqlshell.sh location
SPLICE_HOME=/usr/bin

# change database connection parameters
SPLICE_HOST='localhost'
SPLICE_PORT=1527
SPLICE_USERNAME=splice
SPLICE_PSWD=admin
KERBEROS=false
KERBEROS_PRINCIPAL=splice@SPLICEMACHINE.COLO
KERBEROS_KEYTAB=/tmp/splice.keytab

# choose the data set scale factor: 2, 25, 250, 1000, 10k
SCALE=1000

# CSVs from a public S3 bucket (scale factors up to 1000)
#SOURCE=s3a://splice-benchmark-data/flat/HTAP/htap-$SCALE

# Database backup in a public S3 bucket (scale factors 1000 and 10000)
SOURCE=s3a://splice-benchmark-data/database/HTAP/$SCALE

# CSVs from localFS/HDFS (make sure data is present in the file system)
#SOURCE=/data/htap/htap-$SCALE

# benchmark parameters
TIME=600
CWORKERS=$SCALE
HWORKERS=4

