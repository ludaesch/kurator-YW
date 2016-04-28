import sys
import csv
import time
from datetime import datetime
import re

##################################################################################################
# @BEGIN clean_data_using_localDB
# @PARAM input_data_file_name
# @PARAM cleaned_data_file_name
# @PARAM rejected_data_file_name
# @PARAM log_data_file_name
# @PARAM input_field_delimiter
# @PARAM output_field_delimiter
# @PARAM log_field_delimiter
# @IN input_data @FILE file:{input_data_file_name}
# @OUT cleaned_data  @FILE file:{cleaned_data_file_name}
# @OUT rejected_data @FILE file:{rejected_data_file_name}
# @OUT log_data @FILE file:{log_data_file_name}

def clean_data_using_localDB(
    input_data_file_name, 
    localDB_data_file_name,
    output_data_file_name,
    log_data_file_name,
    input_field_delimiter=',',
    localDB_field_delimiter=',',
    output_field_delimiter=','
    ):  
    
    accepted_record_count = 0
    rejected_record_count = 0
    log_record_count = 0
    output_record_count = 0

    ##############################################################################################
    # @BEGIN read_input_data_records
    # @PARAM input_data_file_name
    # @PARAM input_field_delimiter
    # @IN input_data @FILE file:{input_data_file_name}
    # @OUT original_record
    
    # create log file 
    log_data = open(log_data_file_name,'w')    
    log_data.write(timestamp("Reading input records from '{0}'.\n".format(input_data_file_name)))

    # create CSV reader for localDB records
    localDB_data = csv.DictReader(open(localDB_data_file_name, 'r'),
                                delimiter=localDB_field_delimiter)
    # fieldnames/keys of original input data (dictionary)
    localDB_data_fieldnames = localDB_data.fieldnames
    # find corresponding column position for specified header
    scientificName_pos = fuzzymatch(localDB_data_fieldnames,'name')
    authorship_pos = fuzzymatch(localDB_data_fieldnames,'author')
    eventDate_pos = fuzzymatch(localDB_data_fieldnames,'date')
    locality_pos = fuzzymatch(localDB_data_fieldnames,'locality')
    state_pos = fuzzymatch(localDB_data_fieldnames,'state')
    geography_pos = fuzzymatch(localDB_data_fieldnames,'geography')
    
    # iterate over localDB data records
    localDB_record_num = 0
    
    # find values of specific fields
    localDB_scientificName_lst = []
    localDB_authorship_lst = []
    
    for localDB_record in localDB_data:
        localDB_record_num += 1 
        localDB_scientificName = localDB_record[scientificName_pos.values()[0]]
        localDB_scientificName_lst.append(localDB_scientificName)
        localDB_authorship = localDB_record[authorship_pos.values()[0]]
        localDB_authorship_lst.append(localDB_authorship)
        
    # create CSV reader for input records
    input_data = csv.DictReader(open(input_data_file_name, 'r'),
                                delimiter=input_field_delimiter)

    # iterate over input data records
    record_num = 0
    
    # open file for storing output data if not already open
    output_data = csv.DictWriter(open(output_data_file_name, 'w'), 
                                      input_data.fieldnames, 
                                      delimiter=output_field_delimiter)
    output_data.writeheader()
    output_record_count = 0
    
    for original_record in input_data:
        output_record = original_record
        record_num += 1
        print
        log_data.write('\n')
        log_data.write(timestamp('Reading input record {0:03d}.\n'.format(record_num)))
    
    # @END read_input_data_records

    ##############################################################################################
    # @BEGIN extract_record_fields 
    # @IN original_record
    # @OUT original_scientific_name
    # @OUT original_authorship
    
        # extract values of fields to be validated
        original_scientific_name = original_record['scientificName']
        original_authorship = original_record['scientificNameAuthorship']
            
    # @END extract_record_fields 

        
    ##############################################################################################
    # @BEGIN find_matching_localDB_record 
    # @IN original_scientific_name
    # @OUT matching_localDB_record
    # @OUT localDB_lsid
    
        localDB_match_result = None
        
        # first try exact match of the scientific name against localDB
        log_data.write(timestamp("Trying localDB EXACT match for scientific name: '{0}'.\n".format(original_scientific_name)))
        matching_localDB_record = exactmatch(localDB_scientificName_lst, original_scientific_name)
        if matching_localDB_record is not None:
            log_data.write(timestamp('localDB EXACT match was SUCCESSFUL.\n'))
            localDB_match_result = 'exact'

        # otherwise try a fuzzy match
        else:
            log_data.write(timestamp('EXACT match FAILED.\n'))
            log_data.write(timestamp("Trying localDB FUZZY match for scientific name: '{0}'.\n".format(original_scientific_name)))
            matching_localDB_record = fuzzymatch(localDB_scientificName_lst, original_scientific_name)
            
            if None not in matching_localDB_record.values():
                log_data.write(timestamp('localDB FUZZY match was SUCCESSFUL.\n'))
                localDB_match_result = 'fuzzy'
            else:
                log_data.write(timestamp('localDB FUZZY match FAILED.\n'))
                
            
                    
    # @END find_matching_localDB_record

    ##############################################################################################
    # @BEGIN reject_records_not_in_localDB
    # @IN original_record
    # @IN matching_localDB_record
    # @PARAM rejected_data_file_name
    # @PARAM output_field_delimiter
    # @OUT rejected_data @FILE file:{rejected_data_file_name}
    
        # reject the currect record if not matched successfully against localDB
        if localDB_match_result is None:
            log_data.write(timestamp('REJECTED record {0:03d}.\n'.format(record_num)))
            rejected_record_count += 1
            
            # write output record to output file
            output_data.writerow(output_record)
            output_record_count += 1
            
            # skip to processing of next record in input file
            continue
                
    # @END reject_records_not_in_localDB

    ##############################################################################################
    # @BEGIN update_scientific_name
    # @IN original_scientific_name
    # @IN matching_localDB_record
    # @OUT updated_scientific_name
        
        updated_scientific_name = None
        
        # get scientific name from localDB record if the taxon name match was fuzzy
        if localDB_match_result == 'fuzzy':
            updated_scientific_name = matching_localDB_record['original_scientific_name']
    # @END update_scientific_name

    ##############################################################################################
    # @BEGIN update_authorship
    # @IN matching_localDB_record
    # @IN original_authorship
    # @OUT updated_authorship
    
        updated_authorship = None
        
        # get the scientific name authorship from the localDB record if different from input record
        localDB_name_authorship = localDB_authorship_lst[localDB_scientificName_lst.index(matching_localDB_record)]
        if localDB_name_authorship != original_authorship:
            updated_authorship = localDB_name_authorship

    # @END update_authorship

    ##############################################################################################
    # @BEGIN compose_cleaned_record
    # @IN original_record
    # @IN localDB_lsid
    # @IN updated_scientific_name
    # @IN original_scientific_name
    # @IN updated_authorship
    # @IN original_authorship
    # @OUT cleaned_record
        
        if updated_scientific_name is not None:
            log_data.write(timestamp("UPDATING scientific name from '{0}' to '{1}'.\n".format(
                     original_scientific_name, updated_scientific_name)))
            output_record['scientificName'] = updated_scientific_name
            
        if updated_authorship is not None:
            log_data.write(timestamp("UPDATING scientific name authorship from '{0}' to '{1}'.\n".format(
                original_authorship, updated_authorship)))
            output_record['scientificNameAuthorship'] = updated_authorship
                
    # @END compose_cleaned_record

    ##############################################################################################
    # @BEGIN write_clean_data_set
    # @PARAM cleaned_data_file_name
    # @PARAM output_field_delimiter
    # @IN cleaned_record
    # @OUT cleaned_data  @FILE file:{cleaned_data_file_name}

    
        log_data.write(timestamp('ACCEPTED record {0:03d}.\n'.format(record_num)))
        accepted_record_count += 1
        # write output record to output file
        output_data.writerow(output_record)
        output_record_count += 1
        
    # @END write_clean_data_set

    print
    log_data.write("\n")
    log_data.write(timestamp("Wrote {0} accepted records to '{1}'.\n".format(accepted_record_count, output_data_file_name)))
    log_data.write(timestamp("Wrote {0} rejected records to '{1}'.\n".format(rejected_record_count, output_data_file_name)))

# @END clean_data_using_localDB

def date_validation(
    input_data_file_name2,
    output_data_file_name_date,
    log_data_file_name_date,
    input_field_delimiter=',',
    output_field_delimiter=','
    ):

    accepted_record_count = 0
    rejected_record_count = 0
    log_record_count = 0
    output_record_count = 0
    
    # create log file 
    log_data = open(log_data_file_name_date,'w')    
    log_data.write(timestamp("Reading input records from '{0}'.\n".format(input_data_file_name2)))
    
    match_result = None    
    # create CSV reader for input records
    input_data = csv.DictReader(open(input_data_file_name2, 'r'),
                                delimiter=input_field_delimiter)

    # iterate over input data records
    record_num = 0
    
    # open file for storing output data if not already open
    output_data = csv.DictWriter(open(output_data_file_name_date, 'w'), 
                                      input_data.fieldnames, 
                                      delimiter=output_field_delimiter)
    output_data.writeheader()
    output_record_count = 0
    
    for original_record in input_data:
        output_record = original_record
        record_num += 1
        print
        log_data.write('\n')
        log_data.write(timestamp('Reading input record {0:03d}.\n'.format(record_num)))

        # extract values of fields to be validated
        original_eventDate = original_record['eventDate']
        updated_eventDate = None
                
        # reject the currect record if no value
        if len(original_eventDate) < 1:
            log_data.write(timestamp('Trying validating event date: {0}.\n'.format(original_eventDate)))
            match_result = None
            
        else:
            log_data.write(timestamp("Checking ISO date format (YYYY-MM-DD) for event date: '{0}'.\n".format(original_eventDate)))
            
            # date format: xxxx-xx-xx
            if re.match(r'^(\d{4}\-)+(\d{1,2}\-)+(\d{1,2})$',original_eventDate):
                match_result = 'yes'
            
            # date format: xxxx-xx-xx/xxxx-xx-xx
            elif re.match(r'^(\d{4}\-)+(\d{1,2}\-)+(\d{1,2}\/)+(\d{4}\-)+(\d{1,2}\-)+(\d{1,2})$',original_eventDate):
                match_result = 'yes'
            
            # date format: xx/xx/xx
            elif re.match(r'^(\d{1,2}\/)+(\d{1,2}\/)+(\d{4})$',original_eventDate):
                log_data.write(timestamp('Not compliant with ISO date format.\n'))
                match_result = 'no'
                dateparts_slash = original_eventDate.split('/')
                par0 = dateparts_slash[0]
                par1 = dateparts_slash[1]  
                par2 = dateparts_slash[2]
                par_mon = par0.zfill(2)
                par_day = par1.zfill(2)
                par_yr = par2
                updated_eventDate = par_yr + '-' + par_mon + '-' + par_day
            elif re.match(r'^(\d{1,2}\/)+(\d{1,2}\/)+(\d{2})$',original_eventDate):
                log_data.write(timestamp('Not compliant with ISO date format.\n'))
                match_result = 'no'
                dateparts_slash = original_eventDate.split('/')
                par0 = dateparts_slash[0]
                par1 = dateparts_slash[1]  
                par2 = dateparts_slash[2]
                par_mon = par0.zfill(2)
                par_day = par1.zfill(2)
                par_yr = par2
                prefix_yr = '19'
                fix_yr = prefix_yr + par2
                updated_eventDate = fix_yr + '-' + par_mon + '-' + par_day
      
        if match_result is None:
            log_data.write(timestamp('REJECTED record {0:03d}.\n'.format(record_num)))
            rejected_record_count += 1
        
            # write output record to output file
            output_data.writerow(output_record)
            output_record_count += 1
            
            # skip to processing of next record in input file
            continue
             
        if updated_eventDate is not None:
            log_data.write(timestamp("Converting event date format from '{0}' to '{1}'.\n".format(
                     original_eventDate, updated_eventDate)))
            output_record['eventDate'] = updated_eventDate
            log_data.write(timestamp('ACCEPTED record {0:03d}.\n'.format(record_num)))
            accepted_record_count += 1   
            output_data.writerow(output_record)
            output_record_count += 1
        else:
            log_data.write(timestamp('Compliant with ISO date format.\n'))
            log_data.write(timestamp('ACCEPTED record {0:03d}.\n'.format(record_num)))
            accepted_record_count += 1
            output_data.writerow(output_record)
            output_record_count += 1
               
                
        
    print
    log_data.write("\n")
    log_data.write(timestamp("Wrote {0} accepted records to '{1}'.\n".format(accepted_record_count, output_data_file_name_date)))
    log_data.write(timestamp("Wrote {0} rejected records to '{1}'.\n".format(rejected_record_count, output_data_file_name_date)))
        
        

def exactmatch(lst, label_str):
    match_result = None
    matching_record = None
    for key in lst:
        if key.lower() == label_str.lower():
            match_result = 'exact'
            matching_record = key
            return key
        else:
            return None

def fuzzymatch(lst, label_str):
    pos = 0
    for key in lst:
        pos += 1 
        mat_dict = {}
        if len(label_str) > 0 and key.lower().find(label_str) > -1:
            header_name = key
            mat_dict[label_str] = header_name
            break
        else:            
            mat_dict[label_str] = None
    return mat_dict

def timestamp(message):
    current_time = time.time()
    timestamp = datetime.fromtimestamp(current_time).strftime('%Y-%m-%d %H:%M:%S')
    print "{0}  {1}".format(timestamp, message)
    timestamp_message = (timestamp, message)
    return '  '.join(timestamp_message)

if __name__ == '__main__':
    """ Demo of clean_data_using_localDB script """

    clean_data_using_localDB(
        input_data_file_name='demo_input.csv',
        localDB_data_file_name='demo_localDB.csv',
        output_data_file_name='demo_output_ScientificName.csv',
        log_data_file_name='demo_log_ScientificName.txt'
    )
    date_validation(
        input_data_file_name2='demo_output_ScientificName.csv',
        output_data_file_name_date='demo_output_ScientificName_EventDate.csv',
        log_data_file_name_date='demo_log_EventDate.txt'
    )