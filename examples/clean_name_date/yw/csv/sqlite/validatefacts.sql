.help
.tables
.mode csv
.header on


% FACT: check_type(check_type_id, check_type_name).
check_type(1, "self check").
check_type(2, "external check").

% FACT: self_check_type(check_type_id, self_check_tpye_id, self_check_type_name).
self_check_type(1, 1, "text standard").
self_check_type(1, 2, "date standard").
self_check_type(1, 3, "time standard").
self_check_type(1, 4, "number standard").
self_check_type(1, 5, "authorship standard").


% FACT: external_check_type(check_type_id, external_check_tpye_id, external_check_type_name).
external_check_type(2, 1, "local source").
external_check_type(2, 2, "remote source").

% FACT: source_used(external_check_type_name, source_used_id, source_used_name).
source_used("local source", 1, "local authority source").
source_used("remote source", 2, "remote authority source").


% FACT: match_method(match_method_id, match_method_name).
match_method(1, "EXACT").
match_method(2, "FUZZY").

% FACT: matching_method(matching_method_id, matching_method_name).
matching_method(1, "EXACT").
matching_method(2, "FUZZY").


% FACT: match_result(match_result_id, match_result_name).
match_result(1, "SUCESSFUL").
match_result(2, "FAILED").

% FACT: final_result(final_result_id, final_result_name).
final_result(1, "UNABLE to determine the validity").
final_result(2, "ACCEPTED").

% FACT: field_name(field_name_id, field_name_value).
field_name(1,"catalogNumber").
field_name(2,"scientificName").
field_name(3,"scientificNameAuthorship").
field_name(4,"specificEpithet").
field_name(5,"genus").
field_name(6,"family").
field_name(7,"order").
field_name(8,"class").
field_name(9,"phylum").
field_name(10,"kingdom").
field_name(11,"recordedBy").
field_name(12,"eventDate").
field_name(13,"verbatimEventDate").
field_name(14,"locality").
field_name(15,"stateProvince").
field_name(16,"country").
field_name(17,"higherGeography").
field_name(18,"RecordID").



% FACT: annotation(annotation_id, source_id, line_number, tag, keyword, value)
annotation(214, 1, 278, "log", "@log", "{timestamp} Wrote {rejected_record_count} UNABLE-to-determine-validity records to {output1_data_file_name}").

%% FACT: log_template(log_template_id, port_id, entry_template).
log_template(1, 4, "{timestamp} Reading input records from {input1_data_file_name}").
% FACT: modelfacts_log_template(log_template_id, port_id, entry_template, log_annotation_id).
CREATE TABLE modelfacts_log_template (
    log_template_id     INTEGER     NOT NULL    PRIMARY KEY,
    port_id             INTEGER     NOT NULL,
    entry_template      TEXT        NOT NULL,
    log_annotation_id   INTEGER     NOT NULL,
);
.import modelfacts_log_template.csv modelfacts_log_template
-- SELECT * FROM modelfacts_log_template;


% FACT: log_entry(log_entry_id, log_template_id).
log_entry(1, 1).

log_entry(2, 2).
log_entry(3, 3).
log_entry(4, 4).
log_entry(5, 6).
log_entry(6, 5).
...
log_entry(58, 7).
log_entry(59, 8).

CREATE TABLE log_entry (
    log_entry_id        INTEGER    NOT NULL     PRIMARY KEY,
    log_template_id     INTEGER    NOT NULL,    REFERENCES log_template(log_template_id),
); 
.import log_entry.csv log_entry
-- SELECT * FROM log_entry;


% FACT: log_variable(log_variable_id, variable_name, log_template_id).
log_variable(1,"input1_data_file_name", 1).
log_variable(2, "RecordID", 2).
log_variable(3, "check_type", 3).
log_variable(4, "source_used", 3).
log_variable(5, "match_method", 3).
log_variable(6, "field_name", 3).
log_variable(7, "original_scientificName", 3).
log_variable(5, "match_method", 4).
log_variable(8, "match_result", 4). 
log_variable(9, "final_result", 5). 
log_variable(2, "RecordID", 5).
log_variable(6, "field_name", 6).
log_variable(10, "original_value", 6).
log_variable(11, "updated_value", 6).
log_variable(12, "accepted_record_count", 7).
log_variable(13, "output1_data_file_name", 7).
log_variable(14, "rejected_record_count", 8).
log_variable(13, "output1_data_file_name", 8).

CREATE TABLE log_variable (
    log_variable_id     INTEGER     NOT NULL,
    variable_name       TEXT        NOT NULL,
    log_template_id     INTEGER     NOT NULL,   REFERENCES log_template(log_template_id),
    PRIMARY KEY (log_variable_id, log_template_id)
);
.import log_variable.csv log_variable
-- SELECT * FROM log_variable;

% FACT: log_variable_value(log_entry_id, log_variable_id, log_variable_value).
log_variable_value(1, 1, "demo_input.csv").
...
log_variable_value(51, 2, "17477488").
log_variable_value(58, 12, "8").
log_variable_value(58, 13, "demo_output_name_val.csv").
log_variable_value(59, 14, "2").
log_variable_value(59, 13, "demo_output_name_val.csv").

CREATE log_variable_value (
    log_entry_id        INTEGER     NOT NULL    REFERENCES log_entry(log_entry_id),
    log_variable_id     TEXT        NOT NULL    REFERENCES log_variable(log_variable_id),
    log_variable_value  TEXT        NULL,
    PRIMARY KEY (log_entry_id, log_variable_id)
);
.import log_variable_value.csv log_variable_value
-- SELECT * FROM log_variable_value;


% RULE: unique RecordID
RecordID(X) :- log_variable_value(_, 2, X). 

CREATE TABLE RecordID AS
    SELECT DISTINCT log_entry_id
    FROM log_variable_value, log_variable
    WHERE log_variable.variable_name = "RecordID" and log_variable_value(log_variable_id) = log_variable(log_variable_id);
-- SELECT * FROM RecordID;


% RULE: count RecordID
count(W) :- #count{Y: RecordID(Y)} = W. 

SELECT COUNT(RecordID)
FROM RecordID;


% RULE: log_entry_template_variable(log_entry_id, entry_template, variable_name, log_variable_value).
log_entry_template_variable(X, Y, Z, W) :-
	log_entry(X, U), log_template(U, _, Y), log_variable(V, Z, U), log_variable_value(X, V, W).

CREATE TABLE log_entry_template_variable AS
    SELECT log_entry_id, entry_template, variable_name, log_variable_value
    FROM log_entry, log_template, log_variable, log_variable_value
    WHERE log_entry.log_entry_id = log_variable_value.log_entry_id AND log_variable.log_variable_id = log_variable_value.log_variable_id and log_template.log_template_id = log_entry.log_template_id, log_variable.log_template_id = log_entry.log_template_id;
-- SELECT * FROM log_entry_template_variable;

% RULE: record_result(RecordID, final_result).
record_result(X, Y) :- 
	log_entry_template_variable(Z, "{timestamp} {final_result} record {RecordID}", "RecordID", X), log_entry_template_variable(Z, "{timestamp} {final_result} record {RecordID}", "final_result", Y). 
% record_result ANSWER FACTS
record_result("25671466", "ACCEPTED").

CREATE TABLE record_result AS
    SELECT DISTINCT RecordID, log_variable_value AS final_result
    FROM log_entry_template_variable, log_entry_template_variable
    WHERE log_variable = "RecordID"RecordID.RecordID = variable_name  AND entry_template = "{timestamp} {final_result} record {RecordID}";

% RULE: find the set of entry_id for each record: record_entry_set(RecordID, entry_id_start, entry_id_end).
record_entry_set(X, Y, Z) :-
	log_entry_template_variable(Y, "{timestamp} Reading input record {RecordID}", "RecordID", X), log_entry_template_variable(Z, "{timestamp} {final_result} record {RecordID}", "RecordID", X). 
	
% RULE: record_update(RecordID, updated_field_name, old_value, new_value) 
record_update(X, Y, Z, W) :-
	log_entry_template_variable(U, "{timestamp} UPDATING {field_name} from {original_value} to {updated_value}", "field_name", Y), log_entry_template_variable(U, "{timestamp} UPDATING {field_name} from {original_value} to {updated_value}", "original_value", Z), log_entry_template_variable(U, "{timestamp} UPDATING {field_name} from {original_value} to {updated_value}", "updated_value", W), record_entry_set(X, M, N), M<=U, U<=N.
% record_update ANSWER FACTS: record_update(RecordID, updated_field_name, old_value, new_value).
record_update("25671466", "scientificNameAuthorship", "Gmelin, 1791", "(Gmelin, 1791)").
record_update("27888163", "scientificNameAuthorship", " ", "(Gmelin, 1791)").
record_update("18006440", "scientificName", "Placopcten magellanicus", "Placopecten magellanicus").
record_update("18006440", "scientificNameAuthorship", "Gmellin", "(Gmelin, 1791)").
record_update("90079875", "scientificName", "Nodipecten nodsus", "Nodipecten nodosus").
record_update("17477488", "scientificName", "Pecten nodosus", "Nodipecten nodosus").

%FACT: validate_provenance(RecordID, field_name, check_type, source_used, final_result, match_method, match_result). 
validate_provenance(X, Y, Z, U, V, W, K) :-
	record_entry_set(X, M, N), log_entry_template_variable(I, "{timestamp} Trying {check_type} {source_used} {match_method} match for {field_name}: {original_scientificName}", "check_type", Z), log_entry_template_variable(I, "{timestamp} Trying {check_type} {source_used} {match_method} match for {field_name}: {original_scientificName}", "source_used", U), log_entry_template_variable(I, "{timestamp} Trying {check_type} {source_used} {match_method} match for {field_name}: {original_scientificName}", "match_method", W), log_entry_template_variable(I, "{timestamp} Trying {check_type} {source_used} {match_method} match for {field_name}: {original_scientificName}", "field_name", Y), log_entry_template_variable(J, "{timestamp} {match_method} match was {match_result}", "match_method", W), log_entry_template_variable(J, "{timestamp} {match_method} match was {match_result}", "match_result", K), I>=M, I<=N, J>=M, J<=N, record_result(X, V).
% ANSWER FACTS:
validate_provenance("25671466","scientificName","local source","local authority source","ACCEPTED","EXACT","SUCESSFUL").  

