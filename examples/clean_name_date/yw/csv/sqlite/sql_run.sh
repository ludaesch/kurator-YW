#!/bin/bash

# ./sql_run.sh &> sql_run.txt

# remove existing database
rm clean_name_date.db
 
echo "*** Create Database clean_name_date.db from createtables.sql ***"
# create a database 
echo 'sqlite3 clean_name_date.db < createtables.sql'
sqlite3 clean_name_date.db < createtables.sql  

echo ""
# sqlite3 clean_name_date.db
echo "*** Run log queries ***"
# run log queries
echo 'sqlite3 clean_name_date.db ".read queries.sql"'
sqlite3 clean_name_date.db ".read queries.sql"

echo "*** Done! ***"
