import sys
import csv
import time
from datetime import datetime
import re
import uuid

#######################################################################

"""
@begin clean_name_and_date_workflow
@in input1_data @uri file:'demo_input.csv'
@param local_authority_source @uri file:'demo_localDB.csv'
@out name_val_log @uri file:'name_val_log.txt'
@out output2_data  @uri file:'demo_output_name_date_val.csv'
@out log2_data @uri file:'date_val_log.txt'
"""

"""
@begin validate_scientificName_field_of_data
@param local_authority_source @uri 'demo_localDB.csv'
@in input1_data @uri file:'demo_input.csv'
@out output1_data  @uri file:'demo_output_name_val.csv'
@out name_val_log @uri file:'name_val_log.txt'
"""

def validate_scientificName_field_of_data():  
    
    input1_data_file_name='demo_input.csv',
    local_authority_source_file_name='demo_localDB.csv',
    output1_data_file_name='demo_output_name_val.csv',
    name_val_log_file_name='name_val_log.txt'
    
    accepted_record_count = 0
    rejected_record_count = 0
    log_record_count = 0
    output1_record_count = 0

    # create log file 
    """
    @begin initialize_run
    @out name_val_log @uri file:'name_val_log.txt'
        @log "{timestamp} Reading input records from {input1_data_file_name}"
    """
    name_val_log = open(name_val_log_file_name,'w')    
    name_val_log.write(timestamp("Reading input records from '{0}'.\n".format(input1_data_file_name)))
    
    """
    @end initialize_run
    """
    
    """
    @begin read_scientificName
    @param local_authority_source @uri 'demo_localDB.csv'
    @call fieldmatch
    @out local_authority_source_scientificName_lst
    """
    # create CSV reader for local_authority_source records
    local_authority_source = csv.DictReader(open('demo_localDB.csv', 'r'),
                                delimiter=',')
    # fieldnames/keys of original input data (dictionary)
    local_authority_source_fieldnames = local_authority_source.fieldnames
    
    # find corresponding column position for specified header
    scientificName_pos = fieldmatch(local_authority_source_fieldnames,'name')[1]
    authorship_pos = fieldmatch(local_authority_source_fieldnames,'author')[1]
    eventDate_pos = fieldmatch(local_authority_source_fieldnames,'date')[1]
    locality_pos = fieldmatch(local_authority_source_fieldnames,'locality')[1]
    state_pos = fieldmatch(local_authority_source_fieldnames,'state')[1]
    geography_pos = fieldmatch(local_authority_source_fieldnames,'geography')[1]
       
    # iterate over local_authority_source data records
    local_authority_source_record_num = 0
    
    # find values of specific fields
    local_authority_source_scientificName_lst = []
    local_authority_source_authorship_lst = []
    
    for local_authority_source_record in local_authority_source:
        local_authority_source_record_num += 1 
        local_authority_source_scientificName = local_authority_source_record[scientificName_pos]
        local_authority_source_scientificName_lst.append(local_authority_source_scientificName)
        local_authority_source_authorship = local_authority_source_record[authorship_pos]
        local_authority_source_authorship_lst.append(local_authority_source_authorship)
    """
    @end read_scientificName
    """
    
    """
    @begin read_input1_data_records
    @in input1_data @uri file:'demo_input.csv'
    @out original_scientificName @as scientificName
    @out original_authorship @as authorship
    @out RecordID 
    @out original_others @as others
    @out name_val_log @uri file:'name_val_log.txt'
      @log "{timestamp} Reading input record {RecordID}"
    """
    # create CSV reader for input records
    input1_data = csv.DictReader(open('demo_input.csv', 'r'),
                                delimiter=',')

    # iterate over input data records
    record_num = 0
    RecordID_lst = []
    
    # open file for storing output data if not already open
    output1_data = csv.DictWriter(open('demo_output_name_val.csv', 'w'), 
                                      input1_data.fieldnames, 
                                      delimiter=',')
    output1_data.writeheader()
   
    for original1_record in input1_data:
        RecordID = str(uuid.uuid4().fields[-1])[:8]
        RecordID_lst.append(RecordID)
        output1_record = original1_record
        record_num += 1
        print

        # extract values of fields to be validated
        original_catalogNumber = original1_record['id']
        original_scientificName = original1_record['scientificName']
        original_authorship = original1_record['scientificNameAuthorship']

        name_val_log.write('\n')
        name_val_log.write(timestamp("Reading input record '{0}'.\n".format(RecordID)))
        """
        @end read_input1_data_records
        """
        
        """   
        @begin find_matching_local_authority_source_record 
        @in original_scientificName @as scientificName
        @param local_authority_source_scientificName_lst
        @call exactmatch
        @out matching_local_authority_source_record
        @out final_result
        @out name_val_log @uri file:'name_val_log.txt'
            @log "{timestamp} Trying {check_type} {source_used} {match_method} match for {field_name}: {original_scientificName}"
            @log "{timestamp} {match_method} match was {match_result}"
        """
        matching_method = None
        
        
        # first try exact match of the scientific name against local_authority_source
        check_type = "external check"
        source_used = "remote_authority_source"
        match_method = "EXACT"
        name_val_log.write(timestamp("Trying local authority source EXACT match for scientific name: '{0}'.\n".format(original_scientificName)))
        matching_local_authority_source_record = exactmatch(local_authority_source_scientificName_lst, original_scientificName)[1]
        match_result = exactmatch(local_authority_source_scientificName_lst, original_scientificName)[0]
        if match_result == "SUCCESSFUL":
            name_val_log.write(timestamp('EXACT match was SUCCESSFUL.\n'))
            matching_method = match_method
            final_result = match_result

        # otherwise try a fuzzy match
        else:
            match_method = "FUZZY"
            name_val_log.write(timestamp('EXACT match FAILED.\n'))
            name_val_log.write(timestamp("Trying local authority source FUZZY match for scientific name: '{0}'.\n".format(original_scientificName)))
            matching_local_authority_source_record = fieldmatch(local_authority_source_scientificName_lst, original_scientificName)[1]
            match_result = fieldmatch(local_authority_source_scientificName_lst, original_scientificName)[0]
            if match_result == "SUCCESSFUL":
                name_val_log.write(timestamp('FUZZY match was SUCCESSFUL.\n'))
                matching_method = match_method
                final_result = match_result
            else:
                name_val_log.write(timestamp('FUZZY match FAILED.\n'))   
                final_result= match_result
        """
        @end find_matching_local_authority_source_record
        """

    #########################################################
        # reject the currect record if not matched successfully against local_authority_source
        """
        @begin log_rejected_record
        @param RecordID 
        @in final_result
        @out rejected_record_count
        @out name_val_log @uri file:'name_val_log.txt'
            @log "{timestamp} {final_result} record {RecordID}"
        """
        if match_result != "SUCCESSFUL":
            final_result = "UNABLE to determine the validity"
            name_val_log.write(timestamp("UNABLE to determine the validity record '{0}'.\n".format(RecordID)))
            rejected_record_count += 1
            
            # write output record to output file
            output1_data.writerow(output1_record)
            output1_record_count += 1
            
            # skip to processing of next record in input file
            continue
            """
            @end log_rejected_record
            """
  
     #############################################################
        """
        @begin update_scientificName
        @in original_scientificName @as scientificName
        @in matching_local_authority_source_record
        @out updated_scientificName
        """
        updated_scientificName = None
        
        # get scientific name from local_authority_source record if the taxon name match was fuzzy
        if matching_method == 'FUZZY':
            updated_scientificName = matching_local_authority_source_record
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
        """
        @begin log_updated_record
        @in updated_scientificName
        @in update_authorship
        @in original_scientificName @as scientificName
        @in original_authorship @as authorship
        @out name_val_log @uri file:'name_val_log.txt'
            @log "{timestamp} UPDATING {field_name} from {original_value} to {updated_value}"
        """
        if updated_scientificName is not None:
            field_name = "scientificName"
            original_value = original_scientificName
            updated_value = updated_scientificName
            name_val_log.write(timestamp("UPDATING scientific name from '{0}' to '{1}'.\n".format(
                     original_value, updated_value)))
            output1_record['scientificName'] = updated_scientificName
            
        if updated_authorship is not None:
            field_name = "scientificNameAuthorship"
            original_value = original_authorship
            updated_value = updated_authorship
            name_val_log.write(timestamp("UPDATING scientific name authorship from '{0}' to '{1}'.\n".format(
                original_value, updated_value)))
            output1_record['scientificNameAuthorship'] = updated_authorship
        """
        @end log_updated_record
        """
    #####################################################################
        """
        @begin log_accepted_record
        @param RecordID
        @in final_result
        @out accepted_record_count
        @out name_val_log @uri file:'name_val_log.txt'
            @log "{timestamp} {final_result} record {RecordID}"
        """
        name_val_log.write(timestamp("ACCEPTED record '{0}'.\n".format(RecordID)))
        accepted_record_count += 1
        """
        @end log_accepted_record
        """
        
        # write output record to output file
        """
        @begin write_output1_data
        @in input1_data @uri file:'demo_input.csv'
        @in updated_scientificName
        @in updated_authorship
        @out output1_data  @uri file:'demo_output_name_val.csv'
        """
        output1_data.writerow(output1_record)
        output1_record_count += 1
        """
        @end write_output1_data
        """

    """
    @begin log_summary 
    @in accepted_record_count
    @in rejected_record_count
    @out name_val_log @uri file:'name_val_log.txt'
        @log "{timestamp} Wrote {accepted_record_count} ACCEPTED records to {output1_data_file_name}"
        @log "{timestamp} Wrote {rejected_record_count} UNABLE-to-determine-validity records to {output1_data_file_name}"
    """   
    print
    name_val_log.write("\n")
    name_val_log.write(timestamp("Wrote {0} ACCEPTED records to {1}.\n".format(accepted_record_count, output1_data_file_name)))
    name_val_log.write(timestamp("Wrote {0} UNABLE-to-determine-validity records to {1}.\n".format(rejected_record_count, output1_data_file_name)))
    """
    @end log_summary
    """
    return RecordID_lst
"""
@end validate_scientificName_field_of_data
"""

"""
@begin validate_eventDate_field_of_data
@in output1_data  @uri file:'demo_output_name_val.csv'
@out output2_data  @uri file:'demo_output_name_date_val.csv'
@out log2_data @uri file:'date_val_log.txt'
"""
def validate_eventDate_field_of_data():
    
    input2_data_file_name='demo_output_name_val.csv',
    output2_data_file_name='demo_output_name_date_val.csv',
    log2_data_file_name='date_val_log.txt'
    
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
    @in input2_data  @uri file:'demo_output_name_val.csv'
    @out original2_eventDate @as eventDate
    @out RecordID 
    @out original2_others @as others
    """
    input2_data = csv.DictReader(open('demo_output_name_val.csv', 'r'),
                                delimiter=',')

    # iterate over input data records
    record2_num = 0
    
    
    # open file for storing output data if not already open
    output2_data = csv.DictWriter(open('demo_output_name_date_val.csv', 'w'), 
                                      input2_data.fieldnames, 
                                      delimiter=',')
    output2_data.writeheader()
    output2_record_count = 0
    
    RecordID_lst = validate_scientificName_field_of_data()
    for original2_record, ReID in zip(input2_data, RecordID_lst):
        RecordID = ReID
        output2_record = original2_record
        record2_num += 1
        print

        # extract values of fields to be validated
        original2_eventDate = original2_record['eventDate']
        original2_catalogNumber = original2_record['id']

        """
        @log {timestamp} Reading input record {RecordID} 
        """
        log2_data.write('\n')
        log2_data.write(timestamp("Reading input record '{0}'.\n".format(RecordID)))        
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
            log2_data.write(timestamp("Trying validating event date: '{0}'.\n".format(original2_eventDate)))
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
            @log {timestamp} UNABLE to determine the validity of the record {RecordID}
            """
            log2_data.write(timestamp("UNABLE to determine the validity of the record '{0}'.\n".format(RecordID)))
            rejected2_record_count += 1
        
            # write output record to output file
            output2_data.writerow(output2_record)
            output2_record_count += 1
            
            # skip to processing of next record in input file
            continue
             
        if updated2_eventDate is not None:
            """
            @begin write_log2_data
            @param input2_data_file_name  @uri file:'demo_output_name_val.csv'
            @param RecordID 
            @in original2_eventDate @as eventDate
            @in updated2_eventDate @as updated_eventDate
            @out log2_data @uri file:'date_val_log.txt'             
            @log {timestamp} Updating event date format from '{original2_eventDate}' to '{updated2_eventDate}'
            """
            log2_data.write(timestamp("Updating event date format from '{0}' to '{1}'.\n".format(
                     original2_eventDate, updated2_eventDate)))
            log2_data.write(timestamp("ACCEPTED record '{0}'.\n".format(RecordID)))
            """
            @log {timestamp} ACCEPTED record {RecordID}
            @end write_log2_data
            """
            
            """
            @begin write_output2_data
            @in input2_data  @uri file:'demo_output_name_val.csv'
            @in updated2_eventDate @as updated_eventDate
            @out output2_data  @uri file:'demo_output_name_date_val.csv' 
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
            @log {timestamp} ACCEPTED record {RecordID}.
            """
            log2_data.write(timestamp('Compliant with ISO date format.\n'))
            log2_data.write(timestamp("ACCEPTED record '{0}'.\n".format(RecordID)))
            accepted2_record_count += 1
            output2_data.writerow(output2_record)
            output2_record_count += 1

    print
    """
    @log {timestamp} Wrote {accepted2_record_count} accepted records to {output2_data_file_name}
    @log {timestamp} Wrote {rejected2_record_count} UNABLE-to-determine-validity records to {rejected2_data_file_name}
    """
    log2_data.write("\n")
    log2_data.write(timestamp("Wrote {0} accepted records to {1}.\n".format(accepted2_record_count, output2_data_file_name)))
    log2_data.write(timestamp("Wrote {0} UNABLE-to-determine-validity records to {1}.\n".format(rejected2_record_count, output2_data_file_name)))
    

"""
@end validate_eventDate_field_of_data               
"""

"""
@end clean_name_and_date_workflow
"""

"""    
@begin exactmatch
@param lst
@param label_str
@return key
@return None            
"""
def exactmatch(lst, label_str):
    match_result = "FAILED"
    matching_record = None
    for key in lst:
        if key.lower() == label_str.lower():
            match_result = 'SUCCESSFUL'
            matching_record = key
            break
    return [match_result,matching_record]
"""
@end exactmatch
"""

"""
@begin fuzzymatch
@param str1
@param str2
@return [match_result, str1[x_longest - longest: x_longest], x_longest - longest]            
"""
def fuzzymatch(str1, str2):
    match_result = "FAILED"
    len1 = len(str1)
    len2 = len(str2)
    str1 = str1.lower()
    str2 = str2.lower()
        
    counter = [[0] * (1 + len2) for k in range(1 + len1)]
    longest = 0
    x_longest = 0
    for i in range(1, 1 + len(str1)):
        for j in range(1, 1 + len(str2)):
            if str1[i - 1] == str2[j - 1]:
                counter[i][j] = counter[i - 1][j - 1] + 1
                if counter[i][j] > longest:
                    longest = counter[i][j]
                    x_longest = i
            else:
                counter[i][j] = 0
    
    if longest > min(len(str1),len(str2))/2:
        match_result = 'SUCCESSFUL'
    
    return [match_result, str1[x_longest - longest: x_longest], x_longest - longest]
    
"""
@end fuzzymatch
"""
 
"""
@begin fieldmatch
@param lst
@param str2
@call fuzzymatch
@return match_str            
 """
def fieldmatch(lst, str2):
    matching_str = None
    for str in lst: 
        match_result = fuzzymatch(str, str2)[0]
        if match_result == 'SUCCESSFUL':
            matching_str = str
            break
    return [match_result,matching_str]
"""
@end fieldmatch
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
    """ Demo of clean_name_and_date_workflow script """
    validate_eventDate_field_of_data()
