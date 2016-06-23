#!/bin/bash

cd ~/git/kurator-YW/examples/clean_name_date/

# create a version of the script without the YW annotations (to show the "raw" script file in the next step)
grep "@" -v clean_name_date_yw.py | grep -v "\"\"\"" > clean_name_date.py

# python script that does some data quality control operations (validations, enhancements)
vim clean_name_date.py

# same script, but with yw markup to allow extraction of prospective provenance and queries on log files
# YW is language independent, we can use the same markup in any scripting language, and have common semantics for log files across languages
vim clean_name_date_yw.py

# run the script
/opt/downloads/Python-2.7.11/python clean_name_date_yw.py

# here's one of the output files of the script with with proposed enhancements embedded
cat demo_output_name_val.csv

# and here's a log file for the name validation step (there is another one, date_val_log.txt .. )
vim name_val_log.txt

# we could ask some simple questions of this log file with grep - find lines that match a pattern
# say, which records were accepted
grep ACCEPTED name_val_log.txt

# YW markup lets us do some more interesting things
cd yw

# Here's the set of configuration values for yw
cat yw.properties

# we'll have YW extract the markup, create a provenance model, and build a graph
# NOTE: here, running the 'graph' command will run 'extract' and then 'model' first
java -jar ../../../yesworkflow-0.2.1-SNAPSHOT.jar extract
java -jar ../../../yesworkflow-0.2.1-SNAPSHOT.jar model
java -jar ../../../yesworkflow-0.2.1-SNAPSHOT.jar graph

dot -Tpng combined.gv -o combined.png

# The graph gives us a visualization of what the script does at a high level (overall workflow)
display combined.png

# or at a lower level (individual subworkflows)
java -jar ../../yesworkflow-0.2.1-SNAPSHOT.jar graph ../clean_name_date_yw.py -c graph.subworkflow=clean_name_and_date_workflow.clean_scientific_name -c graph.dotfile=subgraph_name_val.gv 

display subgraph_name_val.png



# And, because we added a model to the log queries, we can query the log files - getting out structured information targeted to our questions
# not just matching lines as with grep.

# Reconstruct a set if facts out of the log files as a set of csv files, and load them into a database.

java -jar ../../yesworkflow-0.2.1-SNAPSHOT.jar recon

cd csv/sqlite

# Load the csv into a sqlite database
sqlite3 clean_name_date.db < loaddata.sql

# list of defined queries:

vim queries.sql

sqlite3 clean_name_date.db < queries.sql

cat queries_output.txt

# Query for the result of one of the queries:
sqlite3 clean_name_date.db

SELECT * from record_update;

SELECT DISTINCT new_value FROM record_update WHERE field_name = 'scientificName';

SELECT DISTINCT u1.log_variable_value AS v1, u2.log_variable_value AS v2, u3.log_variable_value AS v3, u4.log_variable_value AS v4
FROM log_template_variable_name_value u1, log_template_variable_name_value u2, log_template_variable_name_value u3, log_template_variable_name_value u4
WHERE u1.variable_name = "field_name" and u2.variable_name='check_type' and u3.variable_name='source_used' and u4.variable_name="match_method"
AND u1.log_entry_id=u2.log_entry_id and u2.log_entry_id=u3.log_entry_id and u3.log_entry_id=u4.log_entry_id;

# example of a query that extracts sql update statements from the log files...
select 'update taxonomy set name = "'|| new_value || 'where taxonid = ' || record_id  from record_update where field_name = 'scientificName';
