### adm-to-bytes ###
* Syntax:

        adm-to-bytes(var1, number)


 * Displays the annotated bytes of a data value. The main goal with this function is to debug the object
 serialization process in AsterixDB and to show how data being stored look like. Its mainly used for debugging purposes.
 * Arguments:
    * `var1`: a data value, including record, list, string, and number.
    * `number`: a number value from 0 to infinty ("INF") specifying the nested level of a record.
    `number = 0` prints the raw byte array value, `number = 1` or higher prints the byte arrays with
    corresponding annotations. This function is mainly used for debugging.
 * Return Value:
    * A record showing the raw byte array or annotaded byte arrays.


 * Example:

        let $r1 := {"id":1,
            "project":"AsterixDB",
            "address":{"city":"Irvine", "state":"CA"},
            "related":["Hivestrix", "Preglix", "Apache VXQuery"] }
        return adm-to-bytes($r1, <Level>);

 * The expected results are:

     Level=0:

        { "RawBytes": "[24, 0, 0, 0, -127, 0, 0, 0, 4, 0, 0, 0, 25, 0, 0, 0, 33, 0, 0, 0, 44,
         0, 0, 0, 72, 0, 0, 0, 0, 0, 0, 0, 1, 0, 9, 65, 115, 116, 101, 114, 105, 120, 68, 66,
         0, 0, 0, 29, 0, 0, 0, 2, 0, 0, 0, 17, 0, 0, 0, 25, 0, 6, 73, 114, 118, 105, 110, 101,
         0, 2, 67, 65, 13, 0, 0, 0, 58, 0, 0, 0, 3, 0, 0, 0, 22, 0, 0, 0, 33, 0, 0, 0, 42, 0,
         9, 72, 105, 118, 101, 115, 116, 114, 105, 120, 0, 7, 80, 114, 101, 103, 108, 105, 120,
         0, 14, 65, 112, 97, 99, 104, 101, 32, 86, 88, 81, 117, 101, 114, 121]" }

     Level=1:

        { "Tag": "[24]", "Length": "[0, 0, 0, -127]", "NumberOfClosedFields": "[0, 0, 0, 4]",
         "ClosedFieldOffsets": "[0, 0, 0, 25, 0, 0, 0, 33, 0, 0, 0, 44, 0, 0, 0, 72]",
         "ClosedFields": "[0, 0, 0, 0, 0, 0, 0, 1, 0, 9, 65, 115, 116, 101, 114, 105, 120,
         68, 66, 0, 0, 0, 29, 0, 0, 0, 2, 0, 0, 0, 17, 0, 0, 0, 25, 0, 6, 73, 114, 118, 105,
         110, 101, 0, 2, 67, 65, 13, 0, 0, 0, 58, 0, 0, 0, 3, 0, 0, 0, 22, 0, 0, 0, 33, 0, 0,
         0, 42, 0, 9, 72, 105, 118, 101, 115, 116, 114, 105, 120, 0, 7, 80, 114, 101, 103,
         108, 105, 120, 0, 14, 65, 112, 97, 99, 104, 101, 32, 86, 88, 81, 117, 101, 114, 121]" }

      Level=2:

        { "id": { "Tag": "[4]", "Value": "[0, 0, 0, 0, 0, 0, 0, 1]" }, "project": { "Tag": "[13]",
         "Value": "[65, 115, 116, 101, 114, 105, 120, 68, 66]", "Length": "[9]" }, "address":
         { "Tag": "[24]", "Length": "[0, 0, 0, 29]", "NumberOfClosedFields": "[0, 0, 0, 2]",
         "ClosedFieldOffsets": "[0, 0, 0, 17, 0, 0, 0, 25]", "ClosedFields": "[0, 6, 73, 114, 118,
         105, 110, 101, 0, 2, 67, 65]" }, "related": { "Tag": "[22]", "ItemType": "[13]",
         "Length": "[0, 0, 0, 58]", "NumberOfItems": "[0, 0, 0, 3]", "ItemOffsets": "[0, 0, 0, 22,
         0, 0, 0, 33, 0, 0, 0, 42]", "Value": "[0, 9, 72, 105, 118, 101, 115, 116, 114, 105, 120,
         0, 7, 80, 114, 101, 103, 108, 105, 120, 0, 14, 65, 112, 97, 99, 104, 101, 32, 86, 88, 81,
         117, 101, 114, 121]" } }

      Level>=3 (including "INF"):

        { "id": { "Tag": "[4]", "Value": "[0, 0, 0, 0, 0, 0, 0, 1]" }, "project": { "Tag": "[13]",
        "Length": "[9]", "Value": "[65, 115, 116, 101, 114, 105, 120, 68, 66]" },
        "address": { "city": { "Tag": "[13]", "Length": "[0, 6]", "Value": "[73, 114, 118, 105,
        110, 101]" }, "state": { "Tag": "[13]", "Length": "[0, 2]", "Value": "[67, 65]" } },
        "related": { "Tag": "[22]", "ItemType": "[13]", "Length": "[0, 0, 0, 58]", "NumberOfItems":
        "[0, 0, 0, 3]", "ItemOffsets": "[0, 0, 0, 22, 0, 0, 0, 33, 0, 0, 0, 42]", "Value": "[0, 9,
        72, 105, 118, 101, 115, 116, 114, 105, 120, 0, 7, 80, 114, 101, 103, 108, 105, 120, 0, 14,
        65, 112, 97, 99, 104, 101, 32, 86, 88, 81, 117, 101, 114, 121]" } }
