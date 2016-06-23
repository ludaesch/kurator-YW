-- .help
-- .tables
.header on
.mode column
.width 20 20 20
.output queries_output.txt

-- queries
-- LQ0: Which records are finally ACCEPTED after both steps of scientific name cleaning and event date cleaning? - lq0(RecordID).
SELECT DISTINCT r1.record_id 
FROM log_record_result r1, log_record_result r2 
WHERE r1.resource_id != r2.resource_id AND r1.final_result=r2.final_result 
    AND r1.final_result=' ACCEPTED' AND r1.record_id=r2.record_id;

-- LQ1: How many records required corrections? - lq1(#count).
SELECT count(DISTINCT record_id) FROM record_update;


-- LQ2: How many contained problematic values that could not be corrected? - lq2(#count).
SELECT count(DISTINCT record_id)
FROM log_record_result
WHERE final_result = " UNABLE-to-validate";


-- LQ3: Which records were not as good as original? - lq3(record_id)
SELECT DISTINCT record_id FROM log_record_result WHERE resource_id=3 AND final_result = " ACCEPTED"
INTERSECT 
SELECT DISTINCT record_id FROM log_record_result WHERE resource_id=5 AND final_result = " ACCEPTED"
EXCEPT 
SELECT DISTINCT record_id FROM record_update;

-- LQ4: Which records are all the fields that were updated (or determined to be irreparable in any record of the input data set?) - lq4(record_id).
SELECT DISTINCT u1.record_id
FROM record_update u1, record_update u2
WHERE u1.record_id = u2.record_id 
    AND u1. field_name != u2.field_name;


-- LQ5: For a particular field (eg., scientificName) what are unique values for which corrections were proposed? - lq5(old_valiue, new_value).
SELECT DISTINCT new_value
FROM record_update
WHERE field_name = 'scientificName';


-- LQ6: … and the count of each across all records? - lq6(#count).
SELECT count(DISTINCT new_value)
FROM record_update
WHERE field_name = 'scientificName';


-- LQ7: What are all the records that still have problematic values in a particular field and require further attention? - lq7(RecordID)
SELECT DISTINCT record_id
FROM log_record_result
WHERE final_result = " UNABLE-to-validate"; 


-- LQ8: What standards, data sources, or validation services were used to judge the validity of values in a particular field or that provided new values for it? - lq8(field_name, check_type, source_used, match_method).
SELECT DISTINCT u1.log_variable_value AS v1, u2.log_variable_value AS v2, u3.log_variable_value AS v3, u4.log_variable_value AS v4 
FROM log_template_variable_name_value u1, log_template_variable_name_value u2, log_template_variable_name_value u3, log_template_variable_name_value u4 
WHERE u1.variable_name = "field_name" and u2.variable_name='check_type' and u3.variable_name='source_used' and u4.variable_name="match_method" 
AND u1.log_entry_id=u2.log_entry_id and u2.log_entry_id=u3.log_entry_id and u3.log_entry_id=u4.log_entry_id;


-- LQ9: Which records have been updated multiple times in a script and what were those intermediate values? - lq9(RecordID, new_value).
SELECT c1.record_id, new_value as intermidiate_value
FROM 
    (SELECT record_id, count(record_id) AS num
    FROM record_update
    GROUP BY record_id) c1, record_update
WHERE c1.num > 1 AND c1.record_id = record_update.record_id
ORDER BY c1.record_id;

