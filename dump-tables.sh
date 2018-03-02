source dump-tables.vars.sh

mysql -h ${DBLOC} -u ${USER} --password="${PASSW}" ${DBNAME} -e "SHOW TABLES" > ${DATADIR}/${TABLES}

tail --lines=+2 ${DATADIR}/${TABLES} | while IFS='' read -r table
do
  echo "$table" && \
  mysql -h ${DBLOC} -u ${USER} --password="${PASSW}" ${DBNAME} -e "SELECT * FROM $table" > ${DATADIR}/$table.csv && \
  sleep 2;
done
