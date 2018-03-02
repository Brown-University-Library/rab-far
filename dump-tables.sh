source dump-tables.vars.sh

if [ ! -d data ]; then
  mkdir data;
fi

if [ ! -d data/far-tables ]; then
  mkdir data/far-tables;
fi

mysql -h ${DBLOC} -u ${USER} --password="${PASSW}" ${DBNAME} -e "SHOW TABLES" > data/far-tables.txt

tail --lines=+2 data/far-tables.txt | while IFS='' read -r table
do
  echo "$table" && \
  mysql -h ${DBLOC} -u ${USER} --password="${PASSW}" ${DBNAME} -e "SELECT * FROM $table" > data/far-tables/$table.csv && \
  sleep 2;
done
