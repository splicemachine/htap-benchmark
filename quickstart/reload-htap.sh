/* reload htap1000 using my credentials */ 

SPLICE_PSWD=${1:-admin}

./drop-htap.sh splice $SPLICE_PSWD

./load-htap.sh splice $SPLICE_PSWD

