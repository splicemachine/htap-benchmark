#!/bin/bash

usage() {
  echo "Usage: $0 [-j jdbc_url {jdbc:splice://localhost:1527/splicedb}]   [-a action {none}] [-e execute {true}] [-u user {splice}] [-p password {admin}] [-n schemaName {htap}] [-s sample {300}] [-w warehouses {10}] [-t terminals {10}] [-w work_time {600}] [-a rate limited {fales}] [-i weights {\"45,43,4,4,4\"}]"
  echo 
  echo "Examples: "
  echo -e "\t ${0} -j \"jdbc:splice://localhost:1527/splicedb;ssl=basic\" -u splice -p admin -a restore"
  echo
}

show_usage() {
  usage
  echo -e "Options:"
  echo -e "\t-a, --action false\t\tThe action you want to preform.  Valid values: create, destroy, restore, none.  It defaults to none. "
  echo
  echo -e "\t-e, --execute true\t\tExecute the benchmark workload.  Valid values true / false.  It defaults to true. "
  echo
  echo -e "\t-j, --jdbc_url \t\t\tJDBC Url for the splice machine datbase.  It defaults to jdbc:splice://localhost:1527/splicedb "
  echo
  echo -e "\t-u, --user \t\t\tSplice Machine database user.  It defaults to splice "
  echo
  echo -e "\t-p, --password \t\t\tSplice Machine password.  It defaults to admin "
  echo
  echo -e "\t-n, --schemaName \t\tHtap Schema Name.  It defaults to htap "
  echo
  echo -e "\t-r, --schemaUser \t\tHtap Schema User.  It defaults to htap "
  echo
  echo -e "\t-o, --schemaUserPassword \tHtap Schema User password.  It defaults to htapuser "
  echo
  echo -e "\t-d, --restoreDirectory \t\tRestore directory if action is restore.  Defaults to s3a://splice-benchmark-data/database/HTAP/$SCALE "
  echo
  echo -e "\t-b, --backupId \t\t\tBackup id of the restored schema.  Defaults for scale 100 to 2235534337, scale 1000 to 1384221697 and scale 10000 to 39810049 "
  echo
  echo -e "\t-x, --restoreSouceSchema \tSchema of the restored database.  Defaults to htap "
  echo
  echo -e "\t-y, --dataDirectory \t\tIf creating a database, the location of the import files.  Defaults to s3a://splice-benchmark-data/flat/HTAP/htap-$SCALE "
  echo
  echo -e "\t-m, --work_time 3600\t\tThe work time.  It defaults to 300. "
  echo
  echo -e "\t-w, --weights \t\t\tComma delimited list of weights.  It defaults to \"45,43,4,4,4\". "
  echo
  echo -e "\t-i, --interval 1000\t\tThroughput Monitoring Interval in milliseconds.  It defaults to 10000. "
  echo
  echo -e "\t-l, --scale 100\t\t\tScale.  It defaults to 100. "
  echo
  echo -e "\t-t, --tpch_sessions 1400\tTPCH Sessions  It defaults to 4. "
  echo
  echo -e "\t-c, --tpch\c_sessions 1400\tTPCC Sessions  It defaults to 1. "
  echo
}


#
# Define variables and their default values
#
WORK_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"


ACTION=none
EXECUTE=true
JDBC_URL="jdbc:splice://localhost:1527/splicedb"
SPLICE_USER="splice"
SPLICE_PASSWORD="admin"
SCHEMA_NAME="htap"
SCHEMA_USER="htap"
SCHEMA_PASSWORD="htapuser"
BACKUP_DIRECTORY=""
BACKUP_ID=""
RESTORE_SOURCE_SCHEMA="htap"
DATA_DIRECTORY=""

SCALE="100"
HWORKERS="4"
CWORKERS="1"
WORK_TIME="300"
WEIGHTS="45,43,4,4,4"
IM=10000	
SW=300

#
# Process the parameters that were passed in
#
PARAMS=""
while (( "$#" )); do
  case "$1" in
    -s|--sample)
      SW=$2
      shift 2
      ;;
    -j|--jdbc_url)
      JDBC_URL=$2
      shift 2
      ;;
    -u|--user)
      SPLICE_USER=$2
      shift 2
      ;;
    -p|--password)
      SPLICE_PASSWORD=$2
      shift 2
      ;;
    -m|--work_time)
      WORK_TIME=$2
      shift 2
      ;;
    -w|--weights)
      WEIGHTS=$2
      shift 2
      ;;
    -i|--interval)
      IM=$2
      shift 2
      ;;
    -l|--scale)
      SCALE=$2
      shift 2
      ;;
    -t|--tpch_sessions)
      HWORKERS=$2
      shift 2
      ;;
    -c|--tpcc_sessions)
      CWORKERS=$2
      shift 2
      ;;
    -y|--dataDirectory)
      DATA_DIRECTORY=$2
      shift 2
      ;;
    -r|--schemaUser)
      SCHEMA_USER=$2
      shift 2
      ;;
    -n|--schemaName)
      SCHEMA_NAME=$2
      shift 2
      ;;
    -o|--schemaUserPassword)
      SCHEMA_PASSWORD=$2
      shift 2
      ;;
    -d|--restoreDirectory)
      BACKUP_DIRECTORY=$2
      shift 2
      ;;
    -b|--backupId)
      BACKUP_ID=$2
      shift 2
      ;;
    -a|--action)
      ACTION=$2
      shift 2
      ;;
    -e|--execute)
      EXECUTE=$2
      shift 2
      ;;
    -x|--restoreSouceSchema)
      RESTORE_SOURCE_SCHEMA=$2
      shift 2
      ;;    
    --) # end argument parsing
      shift
      break
      ;;
    -*|--*=) # unsupported flags
      echo "Error: Unsupported flag $1" >&2
	  show_usage
      exit 1
      ;;
    *) # preserve positional arguments
      PARAMS="$PARAMS $1"
      shift
      ;;
  esac
done
# set positional arguments in their proper place
eval set -- "$PARAMS"

#
# Setup values for empty variables
#
FULL_JDBC_URL="${JDBC_URL};user=${SPLICE_USER};password=${SPLICE_PASSWORD}"

if [[ "$ACTION" = "create" ]]; then
  if [ -z "$DATA_DIRECTORY" ]
  then
        DATA_DIRECTORY="s3a://splice-benchmark-data/flat/HTAP/htap-${SCALE}"
  fi
fi

if [[ "$ACTION" = "restore" ]]; then
  if [ -z "$BACKUP_DIRECTORY" ]
  then
        BACKUP_DIRECTORY="s3a://splice-benchmark-data/database/HTAP/$SCALE"
  fi
  if [ -z "$BACKUP_ID" ]
  then
    if [[ $SCALE == "100" ]]; then
        BACKUP_ID=2235534337
    elif [[ $SCALE == "1000" ]]; then
        BACKUP_ID=1384221697
    elif [[ $SCALE == "10000" ]]; then
        BACKUP_ID=39810049
    fi
  fi
fi

if [[ "$ACTION" = "destroy" ]]; then
  EXECUTE="false"
fi

#
# Print the variable values
#
echo "##### Splice Benchmark Harness HTAP:"
echo -e "\tACTION=$ACTION"
echo -e "\tEXECUTE=$EXECUTE"
echo
echo -e "\tJDBC_URL=$JDBC_URL"
echo -e "\tSPLICE_USER=$SPLICE_USER"
echo -e "\tSPLICE_PASSWORD=$SPLICE_PASSWORD"
echo -e "\tSCHEMA_NAME=$SCHEMA_NAME"
echo -e "\tSCHEMA_USER=$SCHEMA_USER"
echo -e "\tSCHEMA_PASSWORD=$SCHEMA_PASSWORD"
echo -e "\tRESTORE_SOURCE_SCHEMA=$RESTORE_SOURCE_SCHEMA"
echo -e "\tBACKUP_DIRECTORY=$BACKUP_DIRECTORY"
echo -e "\tBACKUP_ID=$BACKUP_ID"
echo -e "\tDATA_DIRECTORY=$DATA_DIRECTORY"
echo
echo -e "\tSCALE=$SCALE"
echo -e "\tHWORKERS=$HWORKERS"
echo -e "\tCWORKERS=$CWORKERS"
echo -e "\tWORK_TIME=$WORK_TIME"
echo -e "\tWEIGHTS=$WEIGHTS"
echo -e "\tIM=$IM"
echo -e "\tSW=$SW"

#
# Export variables to do variable replacement in config.xml
#
export SPLICE_URL=${JDBC_URL}
export SPLICE_USER=${SCHEMA_USER}
export SPLICE_PASSWORD=${SCHEMA_PASSWORD}
export SCALE=${SCALE}
export WORKERS_TPCC=${CWORKERS}
export WORKERS_TPCH=${HWORKERS}
export WORK_TIME=${WORK_TIME}
export WEIGHTS=${WEIGHTS}

#
# Do variable replacement in config.xml
#
envsubst < "$WORK_DIR/template-config.xml" > $WORK_DIR/config.xml

#
# Set classpath for 
#
HTAP_CLASSPATH=$(echo $WORK_DIR/lib/*.jar | tr ' ' ':'):$WORK_DIR/supportingfiles


#
# Restore the htap schema
#
if [[ "$ACTION" = "create" ]]; then
  java -cp $HTAP_CLASSPATH -Dlog4j.configuration=log4j.properties com.oltpbenchmark.SpliceHtapSchema -a "$ACTION" -j "$FULL_JDBC_URL" -s "$SCHEMA_NAME" -u "$SCHEMA_USER" -p "$SCHEMA_PASSWORD" -d "$DATA_DIRECTORY"
  STATUS=$?
  if [[ "$STATUS" != "0" ]]; then
    echo "There was a problem creating the database"
    exit 1
  fi
fi

#
# Restore the htap schema
#
if [[ "$ACTION" = "restore" ]]; then
  java -cp $HTAP_CLASSPATH -Dlog4j.configuration=log4j.properties com.oltpbenchmark.SpliceHtapSchema -a "$ACTION" -j "$FULL_JDBC_URL" -s "$SCHEMA_NAME" -u "$SCHEMA_USER" -p "$SCHEMA_PASSWORD" -m "$RESTORE_SOURCE_SCHEMA" -b "$BACKUP_DIRECTORY" -i "$BACKUP_ID"
  STATUS=$?
  if [[ "$STATUS" != "0" ]]; then
    echo "There was a problem restoring the database"
    exit 1
  fi
fi

#
# Destroy the htap schema and objects
#
if [[ "$ACTION" = "destroy" ]]; then
  java -cp $HTAP_CLASSPATH -Dlog4j.configuration=log4j.properties com.oltpbenchmark.SpliceHtapSchema -a "$ACTION" -j "$FULL_JDBC_URL" -s "$SCHEMA_NAME" -u "$SCHEMA_USER"
  STATUS=$?
  if [[ "$STATUS" != "0" ]]; then
    echo "There was a problem destroying the database"
    exit 1
  fi
fi

#
# Execute the HTAP benchmark
#
if [[ "$EXECUTE" = "true" ]]; then
  export JSCOPE_CONFIG=/tmp
  SESSION=htap-${SCALE}_${CWORKERS}_${HWORKERS}
  
  java -Xmx31G -cp $HTAP_CLASSPATH -Dlog4j.configuration=log4j.properties com.oltpbenchmark.DBWorkload -b 'tpcc,chbenchmark' -c $WORK_DIR/config.xml --execute=true -im $IM -s $SW -ss -o $SESSION | tee results/$SESSION.out
  STATUS=$?
  if [[ "$STATUS" != "0" ]]; then
    echo "There was a problem running the benchmark"
    exit 1
  fi
fi
