#!/bin/bash

cd ~/git/kurator-YW/examples/clean_name_date/

# make a copy of the script without the yesworkflow markup
grep -v "@" clean_name_date_yw.py | grep -v "\"\"\"" > clean_name_date.py

# python script that does some data quality control operations (validations, enhancements)
vim clean_name_date.py

# same script, but with yw markup to allow extraction of prospective provenance and queries on log files
# YW is language independent, we can use the same markup in any scripting language, and have common semantics for log files across languages
vim clean_name_date_yw.py

# run the script
/opt/downloads/Python-2.7.11/python clean_name_date_yw.py

# here's one of the output files of the script with with proposed enhancements embedded
cat demo_output_name_val.csv

# and here's a log file
vim name_val_log.txt

# we could ask some simple questions of the log file with grep - find lines that match a pattern
# say, which records were accepted
grep ACCEPTED name_val_log.txt

# YW markup lets us do some more interesting things
cd yw

# Here's the set of configuration values for yw
cat yw.properties

# we'll have YW extract the markup, create a provenance model, and build a graph
java -jar ../../../yesworkflow-0.2.1-SNAPSHOT.jar extract
java -jar ../../../yesworkflow-0.2.1-SNAPSHOT.jar model
java -jar ../../../yesworkflow-0.2.1-SNAPSHOT.jar recon
java -jar ../../../yesworkflow-0.2.1-SNAPSHOT.jar graph

dot -Tpng combined.gv -o combined.png

# The graph gives us a visualization of what the script does at a high level
display combined.png

# or at a lower level
display subgraph_name_val.png

cd ..

# And, because we added a model to the log queries, we can query the log files - getting out structured information targeted to our questions
# not just matching lines as with grep.

# list of defined queries:
??????????????????


# query for the result of one of the queries:
??????????????????
