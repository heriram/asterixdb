/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.om.util.admdebugger;

import java.util.EnumSet;
import java.util.Set;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class AnnotatedFieldNameComputerUtil {
    private AnnotatedFieldNameComputerUtil() {
    }

    public static ARecordType getAnnotatedBytesRecordType(ATypeTag typeTag) throws AlgebricksException {
        switch (typeTag) {
            case RECORD:
                return createAnnotatedFieldsRecordType(ByteAnnotationField.recordFields, true);
            case ORDEREDLIST:
            case UNORDEREDLIST:
                return createAnnotatedFieldsRecordType(ByteAnnotationField.listFields, false);
            case STRING:
                return createAnnotatedFieldsRecordType(ByteAnnotationField.stringObjectFields, false);
            case UNION:
                return getNullableAnnotatedFieldsRecordType(true);
            case NULL:
                return getNullableAnnotatedFieldsRecordType(false);
            default:
                return createAnnotatedFieldsRecordType(ByteAnnotationField.asterixObjectFields, true);
        }
    }

    public static ARecordType getNullableAnnotatedFieldsRecordType(boolean isOpen)  throws AlgebricksException {
        try {
            return new ARecordType("NullableAnnotationRecord",
                    new String[] { ByteAnnotationField.TAG.fieldName() }, new IAType[] { BuiltinType.ASTRING }, isOpen);
        } catch (AsterixException | HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    private static ARecordType createAnnotatedFieldsRecordType(Set<ByteAnnotationField> fields, boolean isOpen)
            throws AlgebricksException {
        String fieldNames[] = new String[fields.size()];
        IAType fieldTypes[] = new IAType[fields.size()];
        try {
            int j = 0;
            for (ByteAnnotationField baf : fields) {
                fieldNames[j] = baf.fieldName();
                fieldTypes[j] = BuiltinType.ASTRING;
                j++;
            }
            return new ARecordType("ByteArrayfields", fieldNames, fieldTypes, isOpen);

        } catch (HyracksDataException | AsterixException e) {
            throw new AlgebricksException(e);
        }
    }

    public enum ByteAnnotationField {
        // Common
        TAG("Tag"),

        // FOR RECORDS
        // (required)
        NUMBER_CLOSED_FIELDS("NumberOfClosedFields"),
        CLOSED_FIELD_OFFSETS("ClosedFieldOffsets"),
        CLOSED_FIELDS("ClosedFields"),
        // Optional
        NULLBITMAP("NullBitMap"),
        IS_EXPANDED("IsExpanded"),
        OPENPART_OFFSET("OpenPartOffset"),
        NUMBER_OF_OPEN_FIELDS("NumberOfOpenFields"),
        OPEN_FIELDS_HASH_OFFSET("OpenfieldHashCodeAndOffsets"),
        OPEN_FIELDS("OpenFields"),

        // FOR LISTS
        ITEM_TYPE("ItemType"),
        NUMBER_OF_ITEMS("NumberOfItems"),
        ITEM_OFFSETS("ItemOffsets"),

        // FOR PRIMITIVE TYPES
        VALUE("Value"),

        // For all but numeric values
        LENGTH("Length");

        public static EnumSet<ByteAnnotationField> recordFields = EnumSet.of(TAG, LENGTH,
                NUMBER_CLOSED_FIELDS, CLOSED_FIELD_OFFSETS, CLOSED_FIELDS);
        public static EnumSet<ByteAnnotationField> listFields = EnumSet.of(TAG, ITEM_TYPE, LENGTH, NUMBER_OF_ITEMS,
                ITEM_OFFSETS, VALUE);
        public static EnumSet<ByteAnnotationField> stringObjectFields = EnumSet.of(TAG, LENGTH, VALUE);
        public static EnumSet<ByteAnnotationField> asterixObjectFields = EnumSet.of(TAG, VALUE); // All other objects

        private String fieldName;

        ByteAnnotationField(String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public String toString() {
            return "{" + fieldName + "}";
        }

        public String fieldName() {
            return this.fieldName;
        }
    }
}
