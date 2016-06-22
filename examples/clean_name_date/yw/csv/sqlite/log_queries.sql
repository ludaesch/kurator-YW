/*  created tables from CSV facts import
log_template(log_template_id, port_id, entry_template).
log_entry(log_entry_id, log_template_id).
log_variable(log_variable_id, variable_name, log_template_id).
log_variable_value(log_entry_id, log_variable_id, log_variable_value).
*/
/*  created rule tables
RecordID(RecordID).
log_entry_template_variable(log_entry_id, entry_template, variable_name, log_variable_value).

*/


% LQ0: Which records are ACCEPTED? lq0(RecordID). 
SELECT RecordID 
FROM record_result
WHERE final_result = "ACCEPTED"; 
lq0(X) :- 
	record_result(X, "ACCEPTED").	

% LQ1: How many records required corrections? lq1(#count).
lq1(W) :- 
	#count{Y: record_update(Y, _, _, _)} = W.

% LQ2: How many contained problematic values that could not be corrected? lq2(#count).
lq2(W) :- 
	#count{Y: record_result(Y, "UNABLE to determine the validity")} = W.

% LQ3: How many records were removed? lq3(#count) = lq2
lq3(W) :- 
	#count{Y: record_result(Y, "UNABLE to determine the validity")} = W.

% LQ4: What are all the fields that were updated or determined to be irreparable in any record of the input data set? lq4(updated_field_name).
lq4(X) :- 
	record_update(_, X, _, _).

% LQ5: For a particular field (eg., scientificName) what are unique values for which corrections were proposed? lq5(old_valiue, new_value).
lq5(X, Y) :- 
	record_update(_, "scientificName", X, Y).

% LQ6: … and the count of each across all records? lq6(#count).
lq6(W) :- 
	#count{X: record_update(X, "scientificName", _, _)} = W.

% LQ7: What are all the records that still have problematic values in a particular field and require further attention? lq7(RecordID) 
lq7(X) :- 
	record_result(X, "UNABLE to determine the validity").

% LQ8: What standards, data sources, or validation services were used to judge the validity of values in a particular field or that provided new values for it? lq8(RecordID, field_name, check_type, source_used, match_method).
lq8(X, Y, Z, U, V) :-
	validate_provenance(X, Y, Z, U, _, V, "SUCESSFUL"), record_update(X, Y, _, _). 
	
% LQ9: Which records have been updated multiple times in a script and what were those intermediate values? lq9(RecordID)
lq9(X) :-
	record_update(X, Y, _, _), record_update(X, Z, _, _), Y != Z.

	
