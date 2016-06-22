-- .help
-- .tables
.mode csv
.header on
-- tables to be used for reference
-- FACT: extractfacts_extract_source(source_id, source_path)
CREATE TABLE extractfacts_extract_source (
    source_id           INTEGER     NOT NULL    PRIMARY KEY,
    source_path         TEXT        NOT NULL
);
.import ../extractfacts_extract_source.csv extractfacts_extract_source
-- SELECT * FROM extractfacts_extract_source;


-- FACT: reconfacts_resource(resource_id, resource_uri)
CREATE TABLE reconfacts_resource (
    resource_id         INTEGER     NOT NULL   PRIMARY KEY,
    resource_uri        TEXT     NOT NULL
);
.import ../reconfacts_resource.csv reconfacts_resource
-- SELECT * FROM reconfacts_resource;


-- table facts import from csv

-- FACT: extractfacts_annotation(annotation_id, source_id, line_number, tag, keyword, value)
CREATE TABLE extractfacts_annotation (
    annotation_id       INTEGER     NOT NULL    PRIMARY KEY,
    source_id           INTEGER     NOT NULL    REFERENCES extractfacts_extract_source(source_id),
    line_number         INTEGER     NOT NULL,
    tag                 TEXT        NOT NULL,
    keyword             TEXT        NOT NULL,
    annotation_value    TEXT        NOT NULL
);
.import ../extractfacts_annotation.csv extractfacts_annotation
-- SELECT * FROM extractfacts_annotation;


-- FACT: modelfacts_log_template(log_template_id, port_id, entry_template, log_annotation_id).
CREATE TABLE modelfacts_log_template (
    log_template_id     INTEGER     NOT NULL    PRIMARY KEY,
    port_id             INTEGER     NOT NULL,
    entry_template      TEXT        NOT NULL,
    log_annotation_id   INTEGER     NOT NULL   REFERENCES extractfacts_annotation(annotation_id)
);
.import ../modelfacts_log_template.csv modelfacts_log_template
-- SELECT * FROM modelfacts_log_template;

-- FACT: modelfacts_log_template_variable(log_variable_id, variable_name, log_template_id).
CREATE TABLE modelfacts_log_template_variable (
    log_variable_id     TEXT     NOT NULL    PRIMARY KEY,
    variable_name       TEXT     NOT NULL,
    log_template_id     TEXT     NOT NULL    REFERENCES modelfacts_log_template(log_template_id)
);
.import ../modelfacts_log_template_variable.csv modelfacts_log_template_variable
-- SELECT * FROM modelfacts_log_template;


-- FACT: reconfacts_log_variable_value(resource_id, log_entry_id, log_variable_id, log_variable_value).
CREATE TABLE reconfacts_log_variable_value (
    resource_id         INTEGER     NOT NULL       REFERENCES reconfacts_resource(resource_id),
    log_entry_id        INTEGER     NOT NULL       REFERENCES modelfacts_log_template(log_template_id),       
    log_variable_id     INTEGER     NOT NULL,
    log_variable_value  TEXT        NOT NULL,
    PRIMARY KEY (log_entry_id, log_variable_id)
);
.import ../reconfacts_log_variable_value.csv reconfacts_log_variable_value
-- SELECT * FROM reconfacts_log_variable_value;


-- table rules created for queries 
-- rule: log_template_variable_name(log_template_id, port_id, entry_template, log_variable_id, variable_name, log_annotation_id)
CREATE TABLE log_template_variable_name AS
    SELECT DISTINCT modelfacts_log_template.log_template_id, port_id, entry_template, log_variable_id, variable_name, log_annotation_id
    FROM modelfacts_log_template JOIN modelfacts_log_template_variable
    ON modelfacts_log_template.log_template_id = modelfacts_log_template_variable.log_template_id;
-- SELECT * FROM log_template_variable_name;

-- rule: log_template_variable_name_value(resource_id, log_template_id, entry_template, log_entry_id, log_variable_id, variable_name, log_variable_value). 
CREATE TABLE log_template_variable_name_value AS
    SELECT DISTINCT resource_id, log_template_id, entry_template, log_entry_id, reconfacts_log_variable_value.log_variable_id, variable_name, log_variable_value
    FROM log_template_variable_name JOIN reconfacts_log_variable_value
    ON log_template_variable_name.log_variable_id = reconfacts_log_variable_value.log_variable_id;
-- SELECT * FROM log_template_variable_name_value;

-- rule: log_record_result(RecordID, resource_id, final_result)
CREATE TABLE log_record_result AS
    SELECT DISTINCT l1.log_variable_value as final_result, l1.resource_id as resource_id, l2.log_variable_value as record_id
    FROM log_template_variable_name_value l1, log_template_variable_name_value l2 
    WHERE l1.entry_template='{timestamp} {final_result} the record {RecordID}' 
        AND l1.log_template_id=l2.log_template_id 
        AND l1.variable_name='final_result' 
        AND l2.variable_name='RecordID'
        AND l1.log_entry_id = l2.log_entry_id;
/* SELECT * FROM log_record_result;
 ACCEPTED|3|'20417280'.
 ACCEPTED|3|'43916370'.
 UNABLE-to-validate|3|'15990657'.
 ACCEPTED|3|'14048056'.
 ACCEPTED|3|'33417366'.
 UNABLE-to-validate|3|'96438771'.
 ACCEPTED|3|'20232556'.
 ACCEPTED|3|'12981471'.
 ACCEPTED|3|'17088436'.
 ACCEPTED|3|'23678356'.
 ACCEPTED|5|'20417280'.
 ACCEPTED|5|'43916370'.
 ACCEPTED|5|'15990657'.
 ACCEPTED|5|'14048056'.
 ACCEPTED|5|'33417366'.
 ACCEPTED|5|'96438771'.
 UNABLE-to-validate|5|'20232556'.
 UNABLE-to-validate|5|'12981471'.
 UNABLE-to-validate|5|'17088436'.
 UNABLE-to-validate|5|'23678356'.
 */
-- rule: log_entry_resource(resource_id, log_entry_id)
CREATE TABLE log_entry_resource AS
    SELECT DISTINCT resource_id, log_entry_id
    FROM reconfacts_log_variable_value;
-- SELECT * FROM log_entry_resource;

-- rule: record_update(RecordID, updated_field_name, old_value, new_value)
CREATE TABLE record_update AS
    SELECT v1.log_variable_value as record_id, v2.log_variable_value as field_name, v3.log_variable_value as old_value, v4.log_variable_value as new_value 
    FROM log_template_variable_name_value v1, log_template_variable_name_value v2, log_template_variable_name_value v3, log_template_variable_name_value v4 
    WHERE v1.entry_template='{timestamp} UPDATING record {Record}: {field_name} from {original_value} to {updated_value}' 
        AND v1.entry_template=v2.entry_template AND v2.entry_template=v3.entry_template AND v3.entry_template=v4.entry_template 
        AND v1.variable_name='Record' 
        AND v2.variable_name='field_name' 
        AND v3.variable_name='original_value' 
        AND v4.variable_name='updated_value' 
        AND v1.log_entry_id=v2.log_entry_id AND v2.log_entry_id=v3.log_entry_id and v3.log_entry_id=v4.log_entry_id;
/* SELECT * FROM record_update;
'12706292'|scientificNameAuthorship|'Gmelin, 1791'|'(Gmelin, 1791)'.
'10912053'|scientificNameAuthorship|''|'(Gmelin, 1791)'.
'14337811'|scientificName|'Placopcten magellanicus'|'Placopecten magellanicus'.
'14337811'|scientificNameAuthorship|'Gmellin'|'(Gmelin, 1791)'.
'22933053'|scientificName|'Nodipecten nodsus'|'Nodipecten nodosus'.
'17016145'|scientificName|'Pecten nodosus'|'Nodipecten nodosus'.
'12706292'|eventDate|'5/27/50'|'1950-05-27'.
*/

-- queries
-- LQ0: Which records are finally ACCEPTED after both steps of scientific name cleaning and event date cleaning? lq0(RecordID).
SELECT DISTINCT r1.record_id 
FROM log_record_result r1, log_record_result r2 
WHERE r1.resource_id != r2.resource_id AND r1.final_result=r2.final_result 
    AND r1.final_result=' ACCEPTED' AND r1.record_id=r2.record_id;
/*
ANSWER:
'25422276'.
'14538227'.
'18962800'.
'25118137'.
*/

-- LQ1: How many records required corrections? lq1(#count).
SELECT count(DISTINCT record_id) FROM record_update;
/* ANSWER:
5
*/

-- LQ2: How many contained problematic values that could not be corrected? lq2(#count).
SELECT count(DISTINCT record_id)
FROM log_record_result
WHERE final_result = " UNABLE-to-validate";
/* ANSWER:
6
*/

-- LQ3: How many records were removed? lq3(#count) = lq2
SELECT count(DISTINCT record_id)
FROM log_record_result
WHERE final_result = " UNABLE-to-validate";
/* ANSWER:
6
*/

-- LQ4: What are all the fields that were updated (or determined to be irreparable in any record of the input data set?) lq4(updated_field_name).
SELECT DISTINCT u1.record_id
FROM record_update u1, record_update u2
WHERE u1.record_id = u2.record_id 
    AND u1. field_name != u2.field_name;
/* ANSWER:
'41812057'
'17327705'
*/

-- LQ5: For a particular field (eg., scientificName) what are unique values for which corrections were proposed? lq5(old_valiue, new_value).
SELECT DISTINCT new_value
FROM record_update
WHERE field_name = 'scientificName';
/* ANSWER:
'Placopecten magellanicus'.
'Nodipecten nodosus'.
*/

--LQ6: … and the count of each across all records? lq6(#count).
SELECT count(DISTINCT new_value)
FROM record_update
WHERE field_name = 'scientificName';
/* ANSWER:
2
*/

-- LQ7: What are all the records that still have problematic values in a particular field and require further attention? lq7(RecordID)
SELECT DISTINCT record_id
FROM log_record_result
WHERE final_result = " UNABLE-to-validate"; 
/* ANSWER:
'75849189'.
'13268035'.
'17327705'.
'28426816'.
'20189349'.
'87834150'.
*/

-- LQ8: What standards, data sources, or validation services were used to judge the validity of values in a particular field or that provided new values for it? lq8(RecordID, field_name, check_type, source_used, match_method).
SELECT l1.log_variable_value as resource_id, l2.log_variable_value as check_type, l3.log_variable_value as source_used
FROM log_template_variable_name_value l1, log_template_variable_name_value l2, log_template_variable_name_value l3
WHERE l1.entry_template = "Trying {check_type} {source_used} {match_method} match for validating {field_name}: {field_name_value}"
    AND l1.entry_template = l2.entry_template AND l2.entry_template = l3.entry_template
    AND l1.log_entry_id = l2.log_entry_id AND l2.log_entry_id = l3.log_entry_id;

-- LQ9: Which records have been updated multiple times in a script and what were those intermediate values? lq9(RecordID)
SELECT DISTINCT u1.record_id, u1.new_value, u2.new_value
FROM record_update u1, record_update u2
WHERE u1.record_id = u2.record_id 
    AND u1. field_name != u2.field_name;
/* ANSWER:
'41812057'|'(Gmelin, 1791)'.|'1950-05-27'.
'17327705'|'Placopecten magellanicus'.|'(Gmelin, 1791)'.
'17327705'|'(Gmelin, 1791)'.|'Placopecten magellanicus'.
'41812057'|'1950-05-27'.|'(Gmelin, 1791)'.
*/    

