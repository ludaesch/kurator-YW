# @BEGIN read_input_records
# @OUT record_id
# @OUT …. 
# @LOG {timestamp} Reading input record {record_id}.
# @END read_input_records

# @BEGIN check_event_date
# @IN record_id
# @IN original_event_date
# @OUT corrected_event_date
# @LOG {timestamp} Checking ISO date format (YYYY-MM-DD) for event date: '{event_date}'.
# @LOG {timestamp} Record {record_id} not compliant with ISO date format.
# @LOG {timestamp} Record {record_id} compliant with ISO date format.
# @LOG {timestamp} Converting event date format from '{original_event_date}’ to '{corrected_event_date}'.
# @LOG {timestamp} Event date check ACCEPTED record {record_id}.
# @LOG {timestamp} REJECTED record {record_id}.
...
# @END check_event_date

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
@out output_data  @uri file:{output_data_file_name}
@out log_data @log file:{log_data_file_name}
@param input2_data_file_name
@param output2_data_file_name
@param log2_data_file_name
@param input_field_delimiter
@param output_field_delimiter
@in input2_data @uri file:{input2_data_file_name}
@out output2_data  @uri file:{output2_data_file_name}
@out log2_data @log file:{log2_data_file_name}
"""

"""
@end clean_data_name_date_log
"""

"""
        @begin exactmatch
        @param localDB_scientificName_lst
        @param original_scientific_name
        @out matching_localDB_record
        @out log_data @log file:{log_data_file_name}
        """
"""    
        @end exactmatch
        """


