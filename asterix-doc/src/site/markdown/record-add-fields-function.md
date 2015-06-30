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
