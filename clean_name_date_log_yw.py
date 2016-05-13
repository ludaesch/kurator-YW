import sys
import csv
import time
from datetime import datetime
import re

#######################################################################

"""
@begin clean_name_date_log
@in input1_data @uri file:{input1_data_file_name}
@param local_authority_source @uri {local_authority_source_file_name}
@out log1_data @uri file:{log1_data_file_name}
@out output2_data  @uri file:{output2_data_file_name}
@out log2_data @uri file:{log2_data_file_name}
"""

"""
@begin validate_scientificName_field_of_data
@param local_authority_source @uri {local_authority_source_file_name}
@in input1_data @uri file:{input1_data_file_name}
@out output1_data  @uri file:{output1_data_file_name}
@out log1_data @uri file:{log1_data_file_name}
"""
def validate_scientificName_field_of_data(
    input1_data_file_name, 
    local_authority_source_file_name,
    output1_data_file_name,
    log1_data_file_name,
    ):  
    
    accepted_record_count = 0
    rejected_record_count = 0
    log_record_count = 0
    output1_record_count = 0

    # create log file 
    """
    @log {timestamp} Reading input records from {input1_data_file_name}
    """
    log1_data = open(log1_data_file_name,'w')    
    log1_data.write(timestamp("Reading input records from '{0}'.\n".format(input1_data_file_name)))
    
    """
    @begin read_scientificName
    @param local_authority_source @uri {local_authority_source_file_name}
    @call fuzzymatch
    @out local_authority_source_scientificName_lst
    """
    # create CSV reader for local_authority_source records
    local_authority_source = csv.DictReader(open(local_authority_source_file_name, 'r'),
                                delimiter=',')
    # fieldnames/keys of original input data (dictionary)
    local_authority_source_fieldnames = local_authority_source.fieldnames
    
    # find corresponding column position for specified header
    scientificName_pos = fuzzymatch(local_authority_source_fieldnames,'name')
    authorship_pos = fuzzymatch(local_authority_source_fieldnames,'author')
    eventDate_pos = fuzzymatch(local_authority_source_fieldnames,'date')
    locality_pos = fuzzymatch(local_authority_source_fieldnames,'locality')
    state_pos = fuzzymatch(local_authority_source_fieldnames,'state')
    geography_pos = fuzzymatch(local_authority_source_fieldnames,'geography')
       
    # iterate over local_authority_source data records
    local_authority_source_record_num = 0
    
    # find values of specific fields
    local_authority_source_scientificName_lst = []
    local_authority_source_authorship_lst = []
    
    for local_authority_source_record in local_authority_source:
        local_authority_source_record_num += 1 
        local_authority_source_scientificName = local_authority_source_record[scientificName_pos.values()[0]]
        local_authority_source_scientificName_lst.append(local_authority_source_scientificName)
        local_authority_source_authorship = local_authority_source_record[authorship_pos.values()[0]]
        local_authority_source_authorship_lst.append(local_authority_source_authorship)
    """
    @end read_scientificName
    """
    
    """
    @begin read_input1_data_records
    @in input1_data @uri file:{input1_data_file_name}
    @out original_scientificName @as scientificName
    @out original_authorship @as authorship
    @out original_catalogNumber @as catalogNumber
    @out original_others @as others
    """
    # create CSV reader for input records
    input1_data = csv.DictReader(open(input1_data_file_name, 'r'),
                                delimiter=',')

    # iterate over input data records
    record_num = 0
    
    # open file for storing output data if not already open
    output1_data = csv.DictWriter(open(output1_data_file_name, 'w'), 
                                      input1_data.fieldnames, 
                                      delimiter=',')
    output1_data.writeheader()
   
    for original1_record in input1_data:
        output1_record = original1_record
        record_num += 1
        print

        # extract values of fields to be validated
        original_catalogNumber = original1_record['id']
        original_scientificName = original1_record['scientificName']
        original_authorship = original1_record['scientificNameAuthorship']
        
        """
        @log {timestamp} Reading input record {original_catalogNumber} 
        """
        log1_data.write('\n')
        log1_data.write(timestamp("Reading input record '{0}'.\n".format(original_catalogNumber)))
        """
        @end read_input1_data_records
        """
        
        """   
        @begin find_matching_local_authority_source_record 
        @in original_scientificName @as scientificName
        @param local_authority_source_scientificName_lst
        @call exactmatch
        @OUT matching_local_authority_source_record
        """
        local_authority_source_match_result = None
        
        
        # first try exact match of the scientific name against local_authority_source
        """
        @log {timestamp} Trying {source_type} {source_used} {matching_method} match for {field_name}: {original_scientificName}
        """
        log1_data.write(timestamp("Trying local authority source EXACT match for scientific name: '{0}'.\n".format(original_scientificName)))
        matching_local_authority_source_record = exactmatch(local_authority_source_scientificName_lst, original_scientificName)
        
        if matching_local_authority_source_record is not None:
            """
            @log {timestamp} {source_used} {matching_method} match was {MATCHING_RESULT}
            """
            log1_data.write(timestamp('local authority source EXACT match was SUCCESSFUL.\n'))
            local_authority_source_match_result = 'exact'

        # otherwise try a fuzzy match
        else:
            """
            @log {timestamp} EXACT match FAILED.
            @log {timestamp} Trying {source_used} {matching_method_fuzzy} match for scientific name: {original_scientificName}
            """
            log1_data.write(timestamp('EXACT match FAILED.\n'))
            log1_data.write(timestamp("Trying local authority source FUZZY match for scientific name: '{0}'.\n".format(original_scientificName)))
            matching_local_authority_source_record = fuzzymatch(local_authority_source_scientificName_lst, original_scientificName)
            
            if None not in matching_local_authority_source_record.values():
                log1_data.write(timestamp('local authority source FUZZY match was SUCCESSFUL.\n'))
                local_authority_source_match_result = 'fuzzy'
            else:
                """
                @log {timestamp}  local_authority_source FUZZY match FAILED.
                """
                log1_data.write(timestamp('local_authority_source FUZZY match FAILED.\n'))   
        """
        @end find_matching_local_authority_source_record
        """

    #########################################################
        # reject the currect record if not matched successfully against local_authority_source
        if local_authority_source_match_result is None:
            """
            @log {timestamp} REJECTED record {original_catalogNumber}
            """
            log1_data.write(timestamp("REJECTED record '{0}'.\n".format(original_catalogNumber)))
            rejected_record_count += 1
            
            # write output record to output file
            output1_data.writerow(output1_record)
            output1_record_count += 1
            
            # skip to processing of next record in input file
            continue                
     #############################################################
        """
        @begin update_scientificName
        @in original_scientificName @as scientificName
        @in matching_local_authority_source_record
        @out updated_scientificName
        """
        updated_scientificName = None
        
        # get scientific name from local_authority_source record if the taxon name match was fuzzy
        if local_authority_source_match_result == 'fuzzy':
            updated_scientificName = matching_local_authority_source_record['original_scientificName']
        """
        @end update_scientificName
        """

    #####################################################################
        """
        @begin update_authorship
        @in matching_local_authority_source_record
        @in original_authorship @as authorship
        @out updated_authorship
        """
        updated_authorship = None
        
        # get the scientific name authorship from the local_authority_source record if different from input record
        local_authority_source_name_authorship = local_authority_source_authorship_lst[local_authority_source_scientificName_lst.index(matching_local_authority_source_record)]
        if local_authority_source_name_authorship != original_authorship:
            updated_authorship = local_authority_source_name_authorship

        """
        @end update_authorship
        """

    #####################################################################
        # compose_output1_record
        if updated_scientificName is not None:
            """
            @log {timestamp} UPDATING scientific name from {original_scientificName} to {updated_scientificName}
            """
            log1_data.write(timestamp("UPDATING scientific name from '{0}' to '{1}'.\n".format(
                     original_scientificName, updated_scientificName)))
            output1_record['scientificName'] = updated_scientificName
            
        if updated_authorship is not None:
            """
            @log {timestamp} UPDATING scientific name authorship from {original_authorship} to {updated_authorship}
            """
            log1_data.write(timestamp("UPDATING scientific name authorship from '{0}' to '{1}'.\n".format(
                original_authorship, updated_authorship)))
            output1_record['scientificNameAuthorship'] = updated_authorship

    #####################################################################
        """
        @begin write_log1_data
        @param input1_data_file_name  @uri file:{input1_data_file_name}
        @param original_catalogNumber @as catalogNumber
        @param local_authority_source @uri file:{local_authority_source_file_name} 
        @in original_scientificName @as scientificName
        @in original_authorship @as authorship
        @in updated_scientificName
        @in updated_authorship
        @out log1_data @uri file:{log1_data_file_name}
        @log {timestamp} ACCEPTED record {original_catalogNumber}
        """
        log1_data.write(timestamp("ACCEPTED record '{0}'.\n".format(original_catalogNumber)))
        accepted_record_count += 1
        """
        @end write_log1_data
        """
        
        # write output record to output file
        """
        @begin write_output1_data
        @in input1_data @uri file:{input1_data_file_name}
        @in updated_scientificName
        @in updated_authorship
        @out output1_data  @uri file:{output1_data_file_name}
        @log {timestamp} ACCEPTED record {original_catalogNumber}
        """
        output1_data.writerow(output1_record)
        output1_record_count += 1
        """
        @end write_output1_data
        """

    print
    log1_data.write("\n")
    """
    @log {timestamp} Wrote {accepted_record_count} accepted records to {output1_data_file_name}
    @log {timestamp} Wrote {rejected_record_count} rejected records to {output1_data_file_name}
    """
    log1_data.write(timestamp("Wrote {0} accepted records to '{1}'.\n".format(accepted_record_count, output1_data_file_name)))
    log1_data.write(timestamp("Wrote {0} rejected records to '{1}'.\n".format(rejected_record_count, output1_data_file_name)))
"""
@end validate_scientificName_field_of_data
"""

"""
@begin validate_evenDate_field_of_data
@in output1_data  @uri file:{output1_data_file_name}
@out output2_data  @uri file:{output2_data_file_name}
@out log2_data @uri file:{log2_data_file_name}
"""
def validate_evenDate_field_of_data(
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
    @log {timestamp} Reading input records from {input2_data_file_name}
    """
    log2_data = open(log2_data_file_name,'w')    
    log2_data.write(timestamp("Reading input records from '{0}'.\n".format(input2_data_file_name)))

    match_result = None    
    # create CSV reader for input records
    """
    @begin read_input2_data_records
    @in input2_data  @uri file:{output1_data_file_name}
    @out original2_eventDate @as eventDate
    @out original2_catalogNumber @as catalogNumber
    @out original2_others @as others
    """
    input2_data = csv.DictReader(open(input2_data_file_name, 'r'),
                                delimiter=input_field_delimiter)

    # iterate over input data records
    record2_num = 0
    
    
    # open file for storing output data if not already open
    output2_data = csv.DictWriter(open(output2_data_file_name, 'w'), 
                                      input2_data.fieldnames, 
                                      delimiter=output_field_delimiter)
    output2_data.writeheader()
    output2_record_count = 0
    
    for original2_record in input2_data:
        output2_record = original2_record
        record2_num += 1
        print

        # extract values of fields to be validated
        original2_eventDate = original2_record['eventDate']
        original2_catalogNumber = original2_record['id']

        """
        @log {timestamp} Reading input record {original2_catalogNumber} 
        """
        log2_data.write('\n')
        log2_data.write(timestamp("Reading input record '{0}'.\n".format(original2_catalogNumber)))        
        """
        @end read_input2_data_records
        """
        
        """
        @begin clean_eventDate
        @in original2_eventDate @as eventDate
        @out updated2_eventDate @as updated_eventDate
        """
        updated2_eventDate = None
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
        """
        @end clean_eventDate        
        """
        
        # write into files
        if match2_result is None:
            """
            @log {timestamp} REJECTED record {original2_catalogNumber}
            """
            log2_data.write(timestamp("REJECTED record '{0}'.\n".format(original2_catalogNumber)))
            rejected2_record_count += 1
        
            # write output record to output file
            output2_data.writerow(output2_record)
            output2_record_count += 1
            
            # skip to processing of next record in input file
            continue
             
        if updated2_eventDate is not None:
            """
            @begin write_log2_data
            @param input2_data_file_name  @uri file:{output1_data_file_name}
            @param original2_catalogNumber @as catalogNumber
            @in original2_eventDate @as eventDate
            @in updated2_eventDate @as updated_eventDate
            @out log2_data @uri file:{log2_data_file_name}             
            @log {timestamp} Updating event date format from '{original2_eventDate}' to '{updated2_eventDate}'
            """
            log2_data.write(timestamp("Updating event date format from '{0}' to '{1}'.\n".format(
                     original2_eventDate, updated2_eventDate)))
            log2_data.write(timestamp("ACCEPTED record '{0}'.\n".format(original2_catalogNumber)))
            """
            @log {timestamp} ACCEPTED record {original2_catalogNumber}
            @end write_log2_data
            """
            
            """
            @begin write_output2_data
            @in input2_data
            @in updated2_eventDate @as updated_eventDate
            @out output2_data  @uri file:{output2_data_file_name} 
            """
            output2_record['eventDate'] = updated2_eventDate
            accepted2_record_count += 1   
            output2_data.writerow(output2_record)
            output2_record_count += 1
            """
            @end write_output2_data
            """

        else:
            """
            @log {timestamp} Compliant with ISO date format.
            @log {timestamp} ACCEPTED record {original2_catalogNumber}.
            """
            log2_data.write(timestamp('Compliant with ISO date format.\n'))
            log2_data.write(timestamp("ACCEPTED record '{0}'.\n".format(original2_catalogNumber)))
            accepted2_record_count += 1
            output2_data.writerow(output2_record)
            output2_record_count += 1

    print
    """
    @log {timestamp} Wrote {accepted2_record_count} accepted records to {output2_data_file_name}
    @log {timestamp} Wrote {rejected2_record_count} rejected records to {rejected2_data_file_name}
    """
    log2_data.write("\n")
    log2_data.write(timestamp("Wrote {0} accepted records to '{1}'.\n".format(accepted2_record_count, output2_data_file_name)))
    log2_data.write(timestamp("Wrote {0} rejected records to '{1}'.\n".format(rejected2_record_count, output2_data_file_name)))
    

"""
@end validate_evenDate_field_of_data               
"""

"""
@end clean_name_date_log
"""

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
    """ Demo of clean_name_date_log script """

    validate_scientificName_field_of_data(
        input1_data_file_name='demo_input.csv',
        local_authority_source_file_name='demo_localDB.csv',
        output1_data_file_name='demo_output_name_val.csv',
        log1_data_file_name='name_val_log.txt'
    )
    validate_evenDate_field_of_data(
        input2_data_file_name='demo_output_name_val.csv',
        output2_data_file_name='demo_output_name_date_val.csv',
        log2_data_file_name='date_val_log.txt'
    )