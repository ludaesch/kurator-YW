#!/bin/sh
# ./run_script &> run_script.txt

# for convenience, make YW alias 
alias yw='java -jar yesworkflow-0.2-SNAPSHOT-jar-with-dependencies.jar'

# remove previous output files if exists
rm demo_output_*.csv
rm *.txt

# run Python script
python clean_name_date_yw.py

# save pure Python script without YW markup
grep -v "@" clean_name_date_yw.py | grep -v "\"\"\"" > clean_name_date.py 

# run Yesworkflow to generate prospective provenance (top-level)graph and extractfacts and modelfacts
cd yw/
yw graph

# generate subgraphs for subworkflows
yw graph ../clean_name_date_yw.py -c graph.subworkflow=clean_name_and_date_workflow.clean_scientific_name -c graph.dotfile=subgraph_name_val.gv

yw graph ../clean_name_date_yw.py -c graph.subworkflow=clean_name_and_date_workflow.clean_event_date -c graph.dotfile=subgraph_date_val.gv

# run Yesworkflow to generate reconfacts in CSV format 
yw recon ../clean_name_date_yw.py









