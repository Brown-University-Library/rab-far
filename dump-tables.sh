# Requires user, password, db name, and data directory values
# Expects localhost for MySQL server
source dump-tables.vars.sh 
# Directory for storing output CSV files
far_dir="${DATADIR}/far-tables";

if [ ! -d ${DATADIR} ]; then
  mkdir ${DATADIR};
fi

if [ ! -d ${far_dir} ]; then
  mkdir ${far_dir};
fi

# Necessary for MySQL 'INTO OUTFILE' command
# May be unnecessary with better MySQL configuration
chmod -R 777 ${DATADIR};

# Get table names
mysql -u ${USER} --password="${PASSW}" ${DBNAME} -e "SHOW TABLES" > ${DATADIR}/far-tables.txt

#Iterate through tablenames; 'tail -n +2' skips the first line
tail --lines=+2 ${DATADIR}/far-tables.txt | while IFS='' read -r table
do
  echo "${table}";
  # Get field names
  mysql -u ${USER} --password="${PASSW}" ${DBNAME} -e "SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ',') from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA='${DBNAME}' AND TABLE_NAME='${table}'" > ${DATADIR}/field-data.txt;
  fields=$(tail -1 ${DATADIR}/field-data.txt);
  # Dump table to CSV
  mysql -u ${USER} --password="${PASSW}" ${DBNAME} -e "SELECT * INTO OUTFILE '${far_dir}/${table}.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' FROM ${table}"
  # Write fieldnames to top of each CSV file
  echo "${fields}" | cat - ${far_dir}/${table}.csv > ${far_dir}/${table}.tmp && mv ${far_dir}/${table}.tmp ${far_dir}/${table}.csv;
  sleep 0.5;
done

# Cleanup
rm ${DATADIR}/*.txt;
chmod 775 ${DATADIR};
chmod 775 ${far_dir};