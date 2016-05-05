import sys
import csv
import time
from datetime import datetime
import re

<<<<<<< HEAD
#######################################################################

"""
@begin clean_data_name_date_log
@param input_data_file_name
@param localDB_data_file_name
@param output_data_file_name
@param log_data_file_name
@param input_field_delimiter
@param localDB_field_delimiter
@param output_field_delimiter
@in input_data @uri file:{input_data_file_name}
@in localDB_data @uri {localDB_data_file_name}
@out log_data @uri file:{log_data_file_name}
@param output2_data_file_name 
@param log2_data_file_name
@out output2_data  @uri file:{output2_data_file_name}
@out log2_data @uri file:{log2_data_file_name}
"""
=======
##############################################################################

>>>>>>> kurator-org/master

"""
@begin clean_name_using_localDB
@param input_data_file_name
@param localDB_data_file_name
@param output_data_file_name
@param log_data_file_name
@param input_field_delimiter
@param localDB_field_delimiter
@param output_field_delimiter
<<<<<<< HEAD
@in localDB_data @uri {localDB_data_file_name}
@in input_data @uri file:{input_data_file_name}
@out output_data  @uri file:{output_data_file_name}
@out log_data @uri file:{log_data_file_name}
=======
@in input_data @uri file:{input_data_file_name}
@out output_data  @uri file:{output_data_file_name}
@out log_data @uri file:{log_data_file_name}
@out accepted_record_count
@out rejected_record_count
>>>>>>> kurator-org/master
"""
def clean_name_using_localDB(
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

    # create log file 
    """
<<<<<<< HEAD
    @log {timestamp} Reading input records from {input_data_file_name}
    """
    log_data = open(log_data_file_name,'w')    
    log_data.write(timestamp("Reading input records from '{0}'.\n".format(input_data_file_name)))
    
    """
    @begin read_records_from_localDB
    @in localDB_data @uri {localDB_data_file_name}
    @param localDB_data_file_name
    @param localDB_data_fieldnames
    @param localDB_field_delimiter
    @call fuzzymatch
    @out localDB_scientificName_lst
=======
    @begin create_log_file
    @param input_data_file_name
    @param log_data_file_name
    @call timestamp
    @log {timestamp} Reading input records from {input_data_file_name}
    @out log_data @uri file:{log_data_file_name}
    """
    log_data = open(log_data_file_name,'w')    
    log_data.write(timestamp("Reading input records from '{0}'.\n".format(input_data_file_name)))
    """
    @end create_log_file
    """
    
    """
    @begin read_records_from_localDB
    @param localDB_data_file_name
    @param localDB_data_fieldnames
    @call fuzzymatch
    @out localDB_record_num
    @out localDB_scientificName_lst
    @out localDB_authorship_lst
>>>>>>> kurator-org/master
    """
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
    """
    @end read_records_from_localDB
    """
    
    """
    @begin read_input_data_records
    @param input_data_file_name
    @param input_field_delimiter
    @in input_data @uri file:{input_data_file_name}
<<<<<<< HEAD
    @out original_record
=======
    @call timestamp
    @out record_num @as record_id
    @out original_record
    @out output_record
    @out log_data @uri file:{log_data_file_name}
>>>>>>> kurator-org/master
    """
    # create CSV reader for input records
    input_data = csv.DictReader(open(input_data_file_name, 'r'),
                                delimiter=input_field_delimiter)

    # iterate over input data records
    record_num = 0
<<<<<<< HEAD
      
=======
    
    """
    @begin create_output_data_file
    @param output_data_file_name
    @param output_field_delimiter
    @out output_data
    @out output_record_count
    """
>>>>>>> kurator-org/master
    # open file for storing output data if not already open
    output_data = csv.DictWriter(open(output_data_file_name, 'w'), 
                                      input_data.fieldnames, 
                                      delimiter=output_field_delimiter)
    output_data.writeheader()
    output_record_count = 0
<<<<<<< HEAD
   
=======
    """
    @end create_output_data_file
    """
    
>>>>>>> kurator-org/master
    for original_record in input_data:
        output_record = original_record
        record_num += 1
        print
        """
        @log {timestamp} Reading input record {record_id} 
        """
        log_data.write('\n')
        log_data.write(timestamp('Reading input record {0:03d}.\n'.format(record_num)))   
        """
        @end read_input_data_records
        """
    
        """
<<<<<<< HEAD
        @begin extract_scientific_name_and_authorship_fields
=======
        @begin extract_record_fields
>>>>>>> kurator-org/master
        @in original_record    
        @out original_scientific_name
        @out original_authorship
        """     
<<<<<<< HEAD
        # extract values of fields to be validated
        original_scientific_name = original_record['scientificName']
        original_authorship = original_record['scientificNameAuthorship']
        """
        @end extract_scientific_name_and_authorship_fields
=======
    # extract values of fields to be validated
        original_scientific_name = original_record['scientificName']
        original_authorship = original_record['scientificNameAuthorship']
        """
        @end extract_record_fields
>>>>>>> kurator-org/master
        """
    
        """    
        @begin find_matching_localDB_record 
        @in original_scientific_name
        @in localDB_scientificName_lst
<<<<<<< HEAD
        @call exactmatch
        @OUT matching_localDB_record
        """
=======
        @call timestramp
        @call exactmatch
        @OUT matching_localDB_record
        @OUT localDB_match_result
        @out log_data @uri file:{log_data_file_name}
            """
>>>>>>> kurator-org/master
        localDB_match_result = None
        
        
        # first try exact match of the scientific name against localDB
        """
        @log {timestamp} Trying localDB EXACT match for scientific name: {original_scientific_name}
        """
        log_data.write(timestamp("Trying localDB EXACT match for scientific name: '{0}'.\n".format(original_scientific_name)))
        matching_localDB_record = exactmatch(localDB_scientificName_lst, original_scientific_name)
        
<<<<<<< HEAD
=======
        
        """
        @begin compute_localDB_match_result
        @in matching_localDB_record
        @out localDB_match_result  
        @out log_data @uri file:{log_data_file_name}
        """
>>>>>>> kurator-org/master
        if matching_localDB_record is not None:
            """
            @log {timestamp} localDB EXACT match was SUCCESSFUL
            """
            log_data.write(timestamp('localDB EXACT match was SUCCESSFUL.\n'))
            localDB_match_result = 'exact'

        # otherwise try a fuzzy match
        else:
            """
            @log {timestamp} EXACT match FAILED.
            @log {timestamp} Trying localDB FUZZY match for scientific name: {original_scientific_name}
            """
            log_data.write(timestamp('EXACT match FAILED.\n'))
            log_data.write(timestamp("Trying localDB FUZZY match for scientific name: '{0}'.\n".format(original_scientific_name)))
            matching_localDB_record = fuzzymatch(localDB_scientificName_lst, original_scientific_name)
            
            if None not in matching_localDB_record.values():
                log_data.write(timestamp('localDB FUZZY match was SUCCESSFUL.\n'))
                localDB_match_result = 'fuzzy'
            else:
                """
                @log {timestamp}  localDB FUZZY match FAILED.
                """
<<<<<<< HEAD
                log_data.write(timestamp('localDB FUZZY match FAILED.\n'))   
=======
                log_data.write(timestamp('localDB FUZZY match FAILED.\n'))
        """
        @end compute_localDB_match_result
        """    
>>>>>>> kurator-org/master
        """
        @end find_matching_localDB_record
        """

    #########################################################
<<<<<<< HEAD
=======
        """
        @begin reject_records_not_in_localDB
        @in localDB_match_result
        @out output_data
        @out rejected_record_count
        @out output_record_count
        @out log_data @uri file:{log_data_file_name}
        """
>>>>>>> kurator-org/master
        # reject the currect record if not matched successfully against localDB
        if localDB_match_result is None:
            """
            @log {timestamp} REJECTED record {record_id}
            """
            log_data.write(timestamp('REJECTED record {0:03d}.\n'.format(record_num)))
            rejected_record_count += 1
            
            # write output record to output file
            output_data.writerow(output_record)
            output_record_count += 1
            
            # skip to processing of next record in input file
            continue                
<<<<<<< HEAD
     #############################################################
        """
        @begin update_scientific_name
        @in original_scientific_name
        @in matching_localDB_record
=======
        """
        @end reject_records_not_in_localDB
        """
    #############################################################
        """
        @begin update_scientific_name
        @in original_scientific_name
        @in localDB_match_result
>>>>>>> kurator-org/master
        @out updated_scientific_name
        """
        updated_scientific_name = None
        
        # get scientific name from localDB record if the taxon name match was fuzzy
        if localDB_match_result == 'fuzzy':
            updated_scientific_name = matching_localDB_record['original_scientific_name']
        """
        @end update_scientific_name
        """

    #####################################################################
        """
        @begin update_authorship
        @in matching_localDB_record
        @in original_authorship
<<<<<<< HEAD
=======
        @in localDB_authorship_lst
        @in localDB_scientificName_lst
>>>>>>> kurator-org/master
        @out updated_authorship
        """
        updated_authorship = None
        
        # get the scientific name authorship from the localDB record if different from input record
        localDB_name_authorship = localDB_authorship_lst[localDB_scientificName_lst.index(matching_localDB_record)]
        if localDB_name_authorship != original_authorship:
            updated_authorship = localDB_name_authorship

        """
        @end update_authorship
        """

    #####################################################################
        """
        @begin compose_output_record
        @in updated_scientific_name
<<<<<<< HEAD
        @in original_record
        @in updated_authorship
        @out output_record
=======
        @in original_scientific_name
        @in updated_authorship
        @in original_authorship
        @out output_record
        @out log_data @uri file:{log_data_file_name}
>>>>>>> kurator-org/master
        """
        
        if updated_scientific_name is not None:
            """
            @log {timestamp} UPDATING scientific name from {original_scientific_name} to {updated_scientific_name}
            """
            log_data.write(timestamp("UPDATING scientific name from '{0}' to '{1}'.\n".format(
                     original_scientific_name, updated_scientific_name)))
            output_record['scientificName'] = updated_scientific_name
            
        if updated_authorship is not None:
            """
            @log {timestamp} UPDATING scientific name authorship from {original_authorship} to {updated_authorship}
            """
            log_data.write(timestamp("UPDATING scientific name authorship from '{0}' to '{1}'.\n".format(
                original_authorship, updated_authorship)))
            output_record['scientificNameAuthorship'] = updated_authorship
                
        """
        @end compose_output_record
        """

    #####################################################################
        """
        @begin write_output_data_set
        @param output_data_file_name
        @param output_field_delimiter
        @in output_record
        @out output_data  @uri file:{output_data_file_name}
<<<<<<< HEAD
=======
        @out output_record_count 
        @out accepted_record_count
        @out log_data @uri file: {log_data_file_name}
>>>>>>> kurator-org/master
        @log {timestamp} ACCEPTED record {record_id}
        """
        log_data.write(timestamp('ACCEPTED record {0:03d}.\n'.format(record_num)))
        accepted_record_count += 1
        # write output record to output file
        output_data.writerow(output_record)
        output_record_count += 1
        """
        @end write_output_data_set
        """

    print
    log_data.write("\n")
    """
    @log {timestamp} Wrote {accepted_record_count} accepted records to {output_data_file_name}
    @log {timestamp} Wrote {rejected_record_count} rejected records to {output_data_file_name}
    """
    log_data.write(timestamp("Wrote {0} accepted records to '{1}'.\n".format(accepted_record_count, output_data_file_name)))
    log_data.write(timestamp("Wrote {0} rejected records to '{1}'.\n".format(rejected_record_count, output_data_file_name)))
"""
@end clean_name_using_localDB
"""

"""
@begin date_validation
<<<<<<< HEAD
=======
@param input2_data_file_name @as output_data_file_name
>>>>>>> kurator-org/master
@param output2_data_file_name 
@param log2_data_file_name
@param input_field_delimiter
@param output_field_delimiter
<<<<<<< HEAD
@in output_data @uri file:{output_data_file_name}
@param input2_data_file_name
@out output2_data  @uri file:{output2_data_file_name}
@out log2_data @uri file:{log2_data_file_name}
=======
@in input2_data @uri file:{input2_data_file_name}
@out output2_data  @uri file:{output2_data_file_name}
@out log2_data @uri file:{log2_data_file_name}
@out accepted2_record_count
@out rejected2_record_count
>>>>>>> kurator-org/master
"""
def date_validation(
    input2_data_file_name,
    output2_data_file_name,
    log2_data_file_name,
    input_field_delimiter=',',
    output_field_delimiter=','
    ):
    
    accepted2_record_count = 0
    rejected2_record_count = 0
    log2_record_count = 0
    output2_record_count = 0
    
    # create log file 
    """
<<<<<<< HEAD
    @log {timestamp} Reading input records from {input2_data_file_name}
    """
    log2_data = open(log2_data_file_name,'w')    
    log2_data.write(timestamp("Reading input records from '{0}'.\n".format(input2_data_file_name)))

=======
    @begin create_log2_file
    @param input2_data_file_name
    @param log2_data_file_name
    @call timestamp
    @log {timestamp} Reading input records from {input2_data_file_name}
    @out log2_data @uri file:{log2_data_file_name}
    """
    log2_data = open(log2_data_file_name,'w')    
    log2_data.write(timestamp("Reading input records from '{0}'.\n".format(input2_data_file_name)))
    """
    @end create_log2_file
    """
>>>>>>> kurator-org/master
    match_result = None    
    # create CSV reader for input records
    """
    @begin read2_input_data_records
    @param input2_data_file_name
<<<<<<< HEAD
    @param input_field_delimiter
    @in output_data @uri file:{output_data_file_name}
    @out original2_record
=======
    @param input2_field_delimiter
    @in input2_data @uri file:{input2_data_file_name}
    @call timestamp
    @out record2_num @as record2_id
    @out original2_record
    @out output2_record
    @out log2_data @uri file:{log2_data_file_name}
>>>>>>> kurator-org/master
    """
    input2_data = csv.DictReader(open(input2_data_file_name, 'r'),
                                delimiter=input_field_delimiter)

    # iterate over input data records
    record2_num = 0
    
    
    # open file for storing output data if not already open
<<<<<<< HEAD
=======
    """
    @begin create_output2_data_file
    @param output2_data_file_name
    @param output_field_delimiter
    @out output2_data
    @out output2_record_count
    """
>>>>>>> kurator-org/master
    output2_data = csv.DictWriter(open(output2_data_file_name, 'w'), 
                                      input2_data.fieldnames, 
                                      delimiter=output_field_delimiter)
    output2_data.writeheader()
    output2_record_count = 0
<<<<<<< HEAD
=======
    """
    @end create_output2_data_file
    """
>>>>>>> kurator-org/master
    
    for original2_record in input2_data:
        output2_record = original2_record
        record2_num += 1
        print
        """
        @log {timestamp} Reading input record {record2_id} 
        """
        log2_data.write('\n')
        log2_data.write(timestamp('Reading input record {0:03d}.\n'.format(record2_num)))
        """
        @end read2_input_data_records
        """
        
        # extract values of fields to be validated
        """
<<<<<<< HEAD
        @begin extract_eventDate_fields
=======
        @begin extract_record_fields
>>>>>>> kurator-org/master
        @in original2_record    
        @out original2_eventDate
        """
        original2_eventDate = original2_record['eventDate']
        updated2_eventDate = None
        """
<<<<<<< HEAD
        @end extract_eventDate_fields
=======
        @end extract_record_fields
>>>>>>> kurator-org/master
        """
        
        """
        @begin clean_eventDate
        @in original2_eventDate
        @out log2_data
<<<<<<< HEAD
        @param log2_data_file_name
        @out output2_data
=======
        @out match2_result
        @out updated2_eventDate
        @param log2_data_file_name
        @out rejected2_record_count
        @out output2_record_count
        @out output2_data
        @out output2_record_count
        @out accepted2_record_count
>>>>>>> kurator-org/master
        """
        # reject the currect record if no value
        if len(original2_eventDate) < 1:
            """
            @log {timestamp} Trying validating event date: {original2_eventDate}
            """
            log2_data.write(timestamp('Trying validating event date: {0}.\n'.format(original2_eventDate)))
            match2_result = None
            
        else:
            """
            @log {timestamp} Checking ISO date format (YYYY-MM-DD) for event date: '{original2_eventDate}'
            """
            log2_data.write(timestamp("Checking ISO date format (YYYY-MM-DD) for event date: '{0}'.\n".format(original2_eventDate)))
            
            # date format: xxxx-xx-xx
            if re.match(r'^(\d{4}\-)+(\d{1,2}\-)+(\d{1,2})$',original2_eventDate):
                match2_result = 'yes'
            
            # date format: xxxx-xx-xx/xxxx-xx-xx
            elif re.match(r'^(\d{4}\-)+(\d{1,2}\-)+(\d{1,2}\/)+(\d{4}\-)+(\d{1,2}\-)+(\d{1,2})$',original2_eventDate):
                match2_result = 'yes'
            
            # date format: xx/xx/xx
            elif re.match(r'^(\d{1,2}\/)+(\d{1,2}\/)+(\d{4})$',original2_eventDate):
                """
                @log {timestamp} Not compliant with ISO date format.
                """
                log2_data.write(timestamp('Not compliant with ISO date format.\n'))
                match2_result = 'no'
                dateparts_slash = original2_eventDate.split('/')
                par0 = dateparts_slash[0]
                par1 = dateparts_slash[1]  
                par2 = dateparts_slash[2]
                par_mon = par0.zfill(2)
                par_day = par1.zfill(2)
                par_yr = par2
                updated2_eventDate = par_yr + '-' + par_mon + '-' + par_day
            elif re.match(r'^(\d{1,2}\/)+(\d{1,2}\/)+(\d{2})$',original2_eventDate):
                """
                @log {timestamp} Not compliant with ISO date format.
                """
                log2_data.write(timestamp('Not compliant with ISO date format.\n'))
                match2_result = 'no'
                dateparts_slash = original2_eventDate.split('/')
                par0 = dateparts_slash[0]
                par1 = dateparts_slash[1]  
                par2 = dateparts_slash[2]
                par_mon = par0.zfill(2)
                par_day = par1.zfill(2)
                par_yr = par2
                prefix_yr = '19'
                fix_yr = prefix_yr + par2
                updated2_eventDate = fix_yr + '-' + par_mon + '-' + par_day
      
        if match2_result is None:
            """
            @log {timestamp} REJECTED record {record2_id}
            """
            log2_data.write(timestamp('REJECTED record {0:03d}.\n'.format(record2_num)))
            rejected2_record_count += 1
        
            # write output record to output file
            output2_data.writerow(output2_record)
            output2_record_count += 1
            
            # skip to processing of next record in input file
            continue
             
        if updated2_eventDate is not None:
            """
            @log {timestamp} Converting event date format from '{original2_eventDate}' to '{updated2_eventDate}'
            """
            log2_data.write(timestamp("Converting event date format from '{0}' to '{1}'.\n".format(
                     original2_eventDate, updated2_eventDate)))
            output2_record['eventDate'] = updated2_eventDate
            """
            @log {timestamp} ACCEPTED record {record2_id}
            """
            log2_data.write(timestamp('ACCEPTED record {0:03d}.\n'.format(record2_num)))
            accepted2_record_count += 1   
            output2_data.writerow(output2_record)
            output2_record_count += 1
        else:
            """
            @log {timestamp} Compliant with ISO date format.
            @log {timestamp} ACCEPTED record record2_id.
            """
            log2_data.write(timestamp('Compliant with ISO date format.\n'))
            log2_data.write(timestamp('ACCEPTED record {0:03d}.\n'.format(record2_num)))
            accepted2_record_count += 1
            output2_data.writerow(output2_record)
            output2_record_count += 1
        """
        @end clean_eventDate
        """       
    print
    """
    @log {timestamp} Wrote {accepted2_record_count} accepted records to {output2_data_file_name}
    @log {timestamp} Wrote {rejected2_record_count} rejected records to {rejected2_data_file_name}
    """
    log2_data.write("\n")
    log2_data.write(timestamp("Wrote {0} accepted records to '{1}'.\n".format(accepted2_record_count, output2_data_file_name)))
    log2_data.write(timestamp("Wrote {0} rejected records to '{1}'.\n".format(rejected2_record_count, output2_data_file_name)))
"""
@end date_validation               
"""
<<<<<<< HEAD
"""
@end clean_data_name_date_log
"""
=======

>>>>>>> kurator-org/master

"""    
@begin exactmatch
@param lst
@param label_str
@return key
@return None            
"""
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
"""
@end exactmatch
"""

"""
@begin fuzzymatch
@param lst
@param label_str
@return mat_dict            
"""
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
"""
@end fuzzymatch
"""

"""
@begin timestamp
@param message
@return timestamp_message
"""            
def timestamp(message):
    current_time = time.time()
    timestamp = datetime.fromtimestamp(current_time).strftime('%Y-%m-%d %H:%M:%S')
    print "{0}  {1}".format(timestamp, message)
    timestamp_message = (timestamp, message)
    return '  '.join(timestamp_message)
"""
@end timestamp
"""

if __name__ == '__main__':
    """ Demo of clean_data_name_date_log script """

    clean_name_using_localDB(
        input_data_file_name='demo_input.csv',
        localDB_data_file_name='demo_localDB.csv',
        output_data_file_name='demo_output_ScientificName.csv',
        log_data_file_name='demo_log_ScientificName.txt'
    )
    date_validation(
        input2_data_file_name='demo_output_ScientificName.csv',
        output2_data_file_name='demo_output_ScientificName_EventDate.csv',
        log2_data_file_name='demo_log_EventDate.txt'
    )