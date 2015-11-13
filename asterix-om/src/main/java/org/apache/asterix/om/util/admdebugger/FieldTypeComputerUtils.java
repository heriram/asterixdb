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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FieldTypeComputerUtils {
    public static ARecordType getAnnotatedBytesRecordType(ATypeTag typeTag) {
        String fieldNames[];
        IAType fieldTypes[];

        IAdmBytesField fields[];
        int numFixedFields = 0;

        switch (typeTag) {
            case RECORD:
                fields = RecordField.values();
                numFixedFields = fields.length - RecordField.getNumberNullable();
                break;
            case ORDEREDLIST:
            case UNORDEREDLIST:
                fields = ListField.values();
                numFixedFields = fields.length - ListField.getNumberNullable();
                break;
            default:
                fields = FlatField.values();
                numFixedFields = fields.length - FlatField.getNumberNullable();
        }
        try {
            fieldNames = new String[numFixedFields];
            fieldTypes = new IAType[numFixedFields];
            int j = 0;
            for (int i=0; i<fields.length; i++) {
                if (!fields[i].isNullable()) {
                    fieldNames[j] = fields[i].fieldName();
                    fieldTypes[j] = BuiltinType.ASTRING;
                    j++;
                }
            }
            return new ARecordType("ByteArrayfields", fieldNames, fieldTypes, true);

        } catch (HyracksDataException | AsterixException e) {
            e.printStackTrace();
        }

        return DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
    }


    public enum RecordField implements IAdmBytesField {
        TAG(false, "Tag"), LENGTH(false, "Length"), IS_EXPANDED(true, "IsExpanded"), OPENPART_OFFSET(true, "OpenPartOffset"),
        NUMBER_CLOSED_FIELDS(false, "NumberOfClosedFields"), NULLBITMAP(true, "NullBitMap"),
        CLOSED_FIELD_OFFSETS(false, "ClosedFieldOffsets"), CLOSED_FIELDS(false, "ClosedFields"),
        NUMBER_OF_OPEN_FIELDS(true, "NumberOfOpenFields"), OPEN_FIELDS_HASH_OFFSET(true, "OpenfieldHashCodeAndOffsets"),
        OPEN_FIELDS(true, "OpenFields");

        private boolean isNullable;
        private String fieldName;

        RecordField(boolean isNullable, String fieldName) {
            this.isNullable = isNullable;
            this.fieldName = fieldName;
        }

        public static int numNullableFields = getNumberNullable();
        public static final int getNumberNullable() {
            int num = 0;
            for (IAdmBytesField f: values()) {
                if (f.isNullable()) num++;
            }
            return num;
        }

        public String toString() {
            return "{" + fieldName + ": " + isNullable + "}";
        }

        public String fieldName() {
            return  this.fieldName;
        }

        public boolean isNullable() {
            return  this.isNullable;
        }
    }

    public enum FlatField implements IAdmBytesField {
        TAG(false, "Tag"), LENGTH(true, "Length"), VALUE(false, "Value");

        private boolean isNullable;
        private String fieldName;
        public static int numNullableFields = getNumberNullable();

        FlatField(boolean isNullable, String fieldName) {
            this.isNullable = isNullable;
            this.fieldName = fieldName;
        }

        public static final int getNumberNullable() {
            int num = 0;
            for (IAdmBytesField f: values()) {
                if (f.isNullable()) num++;
            }
            return num;
        }

        public String fieldName() {
            return  this.fieldName;
        }

        public boolean isNullable() {
            return  this.isNullable;
        }

        public String toString() {
            return "{" + fieldName + ": " + isNullable + "}";
        }

    }

    public enum ListField implements IAdmBytesField {
        TAG(false, "Tag"), ITEM_TYPE(false, "ItemType"), LENGTH(false, "Length"), NUMBER_OF_ITEMS(false, "NumberOfItems"),
        ITEM_OFFSETS(false, "ItemOffsets"), VALUE(false, "Value");

        private boolean isNullable;
        private String fieldName;

        public static int numNullableFields = getNumberNullable();

        ListField(boolean isNullable, String fieldName) {
            this.isNullable = isNullable;
            this.fieldName = fieldName;
        }

        public static final int getNumberNullable() {
            int num = 0;
            for (IAdmBytesField f: values()) {
                if (f.isNullable()) num++;
            }
            return num;
        }

        public String fieldName() {
            return  this.fieldName;
        }

        public boolean isNullable() {
            return  this.isNullable;
        }

        public String toString() {
            return "{" + fieldName + ": " + isNullable + "}";
        }

    }

}
