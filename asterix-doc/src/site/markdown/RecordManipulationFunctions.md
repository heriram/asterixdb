#Record manipulation functions#


### remove-fields ###
 * Syntax:


        remove-fields(input_record, field_names)


 * Remove indicated fields from a record given a list of field names.
 * Arguments:
    * `input_record`:  a record value.
    * `field_names`: an ordered list of strings and/or ordered list of ordered list of strings.
                
 * Return Value:
    * A new record value without the fields listed in the second argument.


 * Example:


        let $r1 := {"id":1, 
            "project":"AsterixDB", 
            "address":{"city":"Irvine", "state":"CA"}, 
            "related":["Hivestrix", "Preglix", "Apache VXQuery"] }
        return remove-fields($r1, [["address", "city"], "related"])


 * The expected result is:


	       { "id":1, 
	         "address":{"state":"CA"}, 
	         "project":"AsterixDB"}


### record-merge ###
 * Syntax:


        record-merge(record1, record2)


 * Merge two different records into a new record.
 * Arguments:
    * `record1` : a record value.
    * `record2` : a record value.
 * Return Value:
    * A new record value with fields from both input records. If a field’s names in both records are the same, an exception is issued.


 * Example:


        let $r1 := {"id":1, 
            "project":"AsterixDB", 
            "address":{"city":"Irvine", "state":"CA"}, 
            "related":["Hivestrix", "Preglix", "Apache VXQuery"] }
			
		let $r2 := {"user_id": 22,
           "employer": "UC Irvine",
           "employment-type": "visitor" }  
        return  record-merge($r1, $r2)


 * The expected result is:


		{"id":1, 
		    "project":"AsterixDB", 
		    "address":{"city":"Irvine", "state":"CA"}, 
		    "related":["Hivestrix", "Preglix", "Apache VXQuery"]
		    "user-id": 22,
		    "employer": "UC Irvine",
		    "employment-type": "visitor"}


### add-record-fields ###
 * Syntax:


        add-record-fields(input_record, fields)


 * Add fields from a record given a list of field names.
 * Arguments:
    * `input_record` : a record value.
    * `fields`: an ordered list of field descriptor records where each record has field-name.
 * Return Value:
    * A new record value with the new fields included.


 * Example:


        let $r1 := {"id":1, 
            "project":"AsterixDB", 
            "address":{"city":"Irvine", "state":"CA"}, 
            "related":["Hivestrix", "Preglix", "Apache VXQuery"] }
        return add-record-fields($r1, [{"field-name":"employer", "field-value":"UC Irvine"}])


 * The expected result is:

	      {"id":1, 
	            "project":"AsterixDB", 
	            "address":{"city":"Irvine", "state":"CA"}, 
	            "related":["Hivestrix", "Preglix", "Apache VXQuery"]
	            "employer": "UC Irvine"}