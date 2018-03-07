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
  echo "Get fields: $table";
  mysql -h ${DBLOC} -u ${USER} --password="${PASSW}" ${DBNAME} -e "select GROUP_CONCAT(COLUMN_NAME SEPARATOR '\`,\`') from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA='${DBNAME}' AND TABLE_NAME='${table}'" > data/field-data.txt;
  fields=$(tail -1 data/field-data.txt);
  echo "${table}|\`${fields}\`" >> data/far-fields.txt && \
  sleep 0.5;
done

while IFS='|' read -r table fields
do
  echo "${table} : ${fields}" && \
  mysql -h ${DBLOC} -u ${USER} --password="${PASSW}" ${DBNAME} -e "select CONCAT_WS('~@~',${fields}) FROM ${table}" > data/far-tables/$table.csv && \
  sleep 1;
done < data/far-fields.txt
