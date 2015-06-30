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
