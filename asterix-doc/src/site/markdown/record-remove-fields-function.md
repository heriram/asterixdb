### remove-fields ###
 * Syntax:


        record-remove-fields(input_record, field_names)


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