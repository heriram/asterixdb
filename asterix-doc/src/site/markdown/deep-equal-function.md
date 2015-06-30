### deep-equal ###

* Syntax:


        deep-equal(var1, var2)


 * Assess the equality between two variables (e.g., records and lists).
 * Arguments:
    * `var1` : a data value, such as record and list.
    * `var2`: a data value, such as record and list.
 * Return Value:
    * `true` or `false` depending on the data equality.


 * Example:


		let $r1 := {"id":1, 
		    "project":"AsterixDB", 
		    "address":{"city":"Irvine", "state":"CA"}, 
		    "related":["Hivestrix", "Preglix", "Apache VXQuery"] }
			
		let $r2 := {"id":1, 
		            "project":"AsterixDB", 
		            "address":{"city":"San Diego", "state":"CA"}, 
		            "related":["Hivestrix", "Preglix", "Apache VXQuery"] }
		return deep-equal($r1, $r2)


 * The expected result is:

      	false
