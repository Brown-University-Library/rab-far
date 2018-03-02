source dump-tables.vars.sh

mysql -h ${DBLOC} -u ${USER} --password="${PASSW}" ${DBNAME} -e "SHOW TABLES" > data/far_tables.txt
