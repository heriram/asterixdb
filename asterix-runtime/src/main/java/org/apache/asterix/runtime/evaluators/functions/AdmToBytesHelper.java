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

package org.apache.asterix.runtime.evaluators.functions;

import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.asterix.om.util.admdebugger.AnnotatedFieldNameComputerUtil.ByteAnnotationField;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class AdmToBytesHelper {
    private final int MAX_NUMBER_OF_MARKS = 12; // Max number of marks in a record + 1 for the end mark
    private final int[] marks = new int[MAX_NUMBER_OF_MARKS];
    private final String[] markNames = new String[MAX_NUMBER_OF_MARKS];
    private final StringBuilder valueHolder = new StringBuilder();
    private final PointableAllocator allocator = new PointableAllocator();
    private final IARecordBuilder recordBuilder = new RecordBuilder();
    private final ArrayBackedValueStorage nameBuffer = new ArrayBackedValueStorage();
    private final ArrayBackedValueStorage valueBuffer = new ArrayBackedValueStorage();
    private final IVisitablePointable byteArrayStringPointable = allocator.allocateFieldValue(BuiltinType.ASTRING);
    private final ByteArrayAccessibleOutputStream outputBos = new ByteArrayAccessibleOutputStream();
    private final DataOutputStream outputDos = new DataOutputStream(outputBos);
    private PointableUtils pointableUtils;

    public AdmToBytesHelper(PointableUtils pointableUtils) {
        this.pointableUtils = pointableUtils;
    }

    public ARecordVisitablePointable getAnnotatedByteArray(IVisitablePointable valuePointable) throws AsterixException {
        return getAnnotatedByteArray(valuePointable, DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
    }

    public ARecordVisitablePointable getAnnotatedByteArray(IVisitablePointable valuePointable,
            ARecordType requiredRecType) throws AsterixException {
        allocator.reset();

        IVisitablePointable annotationRecordPointable = allocator.allocateRecordValue(requiredRecType);
        getAnnotatedByteArray(valuePointable, requiredRecType, annotationRecordPointable);
        return (ARecordVisitablePointable) annotationRecordPointable;
    }

    public void getAnnotatedByteArray(IVisitablePointable valuePointable, ARecordType requiredRecType,
            IVisitablePointable annotatedByteArrayPointable) throws AsterixException {
        int markCount = getByteArrayMarkers(valuePointable);

        recordBuilder.reset(requiredRecType);
        recordBuilder.init();
        try {
            for (int i = 0; i < markCount; i++) {
                int id = recordBuilder.getFieldId(markNames[i]);
                setByteArrayStringPointableValue(valuePointable.getByteArray(), marks[i], (marks[i + 1] - marks[i]),
                        byteArrayStringPointable, true);
                if (id >= 0) {
                    recordBuilder.addField(id, byteArrayStringPointable);
                } else {
                    nameBuffer.reset();
                    pointableUtils.serializeString(markNames[i], nameBuffer, true);
                    recordBuilder.addField(nameBuffer, byteArrayStringPointable);
                }
            }
            outputBos.reset();
            recordBuilder.write(outputDos, true);
            annotatedByteArrayPointable.set(outputBos.getByteArray(), 0, outputBos.size());
        } catch (IOException e) {
            throw new AsterixException(e);
        }
    }

    /**
     * Print a byte array as a string
     *
     * @param bytes
     *            The array of bytes
     * @param offset
     *            The start offset for to read from
     * @param length
     *            The number of the bytes to read
     * @param resultPointable
     *            Pointer to where the string is to be stored after serialization
     * @param writeTag
     *            A flag specifying whether we should add a tag byte at the start of the string pointable
     */
    public void setByteArrayStringPointableValue(byte bytes[], int offset, int length,
            IVisitablePointable resultPointable, boolean writeTag) throws AsterixException {
        int end = offset + length;

        valueHolder.setLength(0);
        valueHolder.append("[" + bytes[offset]);
        for (int i = offset + 1; i < end; i++) {
            valueHolder.append(", " + bytes[i]);
        }
        valueHolder.append("]");
        valueBuffer.reset();
        pointableUtils.serializeString(valueHolder.toString(), valueBuffer, writeTag);
        resultPointable.set(valueBuffer.getByteArray(), valueBuffer.getStartOffset(), valueBuffer.getLength());
    }

    private int getByteArrayMarkers(IVisitablePointable valuePointable) throws AsterixException {
        byte[] bytes = valuePointable.getByteArray();
        int offset = valuePointable.getStartOffset();
        int length = valuePointable.getLength();
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[offset]);

        int markCount = 0;
        switch (typeTag) {
            case RECORD:
                markCount = getRecordByteArrayMarkers(bytes, offset,
                        ((ARecordVisitablePointable) valuePointable).getInputRecordType());
                break;
            case UNORDEREDLIST:
            case ORDEREDLIST:
                markCount = getListByteArrayMarkers(bytes, offset);
                break;
            case STRING:
                markCount = getStringByteArrayMarkes(bytes, offset, length);
                break;
            case NULL:
                markCount = setMark(0, ByteAnnotationField.TAG, offset);
                break;
            default:
                markCount = setMark(0, ByteAnnotationField.TAG, offset);
                markCount = setMark(markCount, ByteAnnotationField.VALUE, offset + 1);

        }
        // Set an end mark
        marks[markCount] = offset + length;
        return markCount;
    }

    /**
     * 1 byte - tag, 13
     * x bytes - variable number length bytes
     * y bytes - value of x-length byte array of the value of the string
     *
     */
    private int getStringByteArrayMarkes(byte bytes[], int offset, int length) {
        int s = offset;
        int i = setMark(0, ByteAnnotationField.TAG, s);
        s++;
        i = setMark(i, ByteAnnotationField.LENGTH, s);

        // Get the string length
        int utflen = UTF8StringUtil.getUTFLength(bytes, offset + 1);
        if (utflen > 0) { // If not an empty string
            s = offset + (length - utflen);
            i = setMark(i, ByteAnnotationField.VALUE, s);
        }
        return i;
    }

    /*
     *
     * 1 byte - Orderedlist (or Unorderedlist) tag, 22 (excluded when type is known)
     * 1 byte - Type of items on the list
     * 4 bytes - Total number of bytes
     * 4 bytes - Number of items
     * if (type of list items is string, record, or list (length is not constant))
     *    4 bytes per item (offsets)
     * for each item
     *    Bytes of the item (In the case of a list of ANY, the items will include type tags)
     */
    private int getListByteArrayMarkers(byte bytes[], int offset) throws AsterixException {
        int s = offset;
        int i = setMark(0, ByteAnnotationField.TAG, s);
        s++;
        // 1 byte - Type of items on the list
        i = setMark(i, ByteAnnotationField.ITEM_TYPE, s);
        s++;
        //4 bytes - Total number of bytes
        i = setMark(i, ByteAnnotationField.LENGTH, s);
        s += 4;
        //4 bytes - Number of items
        i = setMark(i, ByteAnnotationField.NUMBER_OF_ITEMS, s);
        int numberOfItems = AInt32SerializerDeserializer.getInt(bytes, s);
        s += 4;
        // 4 bytes for each item
        i = setMark(i, ByteAnnotationField.ITEM_OFFSETS, s);
        s += 4 * numberOfItems;
        // The rest is the values of the items
        return setMark(i, ByteAnnotationField.VALUE, s);
    }

    /*
     *
     * 1 byte - Record tag, 24 (excluded when type is known)
     * 4 bytes - Total number of bytes
     * if (recordType is not closed)
     * 1 byte - Boolean isExpanded
     * if (isExpanded)
     *   4 bytes - Offset to open part
     * 4 bytes - Number of closed fields
     * if (recordType hasNullableFields)
     *   ceil (numberOfFields / 8) bytes - Nullbitmap (1 bit per field, "1" means field is Null for this record)
     * for each closed field that is not Null for this record
     *   4 bytes - Closed field offset
     * for each closed field that is not Null for this record
     *    Bytes of the field (type is known from recordtype, so the bytes will not have a type tag)
     * if (isExpanded)
     *   4 bytes - Number of open fields
     *   for each open field
     *     4 bytes - Hash code
     *     4 bytes - Offset
     *   for each open field
     *     Bytes of the field name (String, no type tag)
     *     Bytes of the field (with type tag)
     *
     */
    private int getRecordByteArrayMarkers(byte bytes[], int offset, ARecordType inputRecType) throws AsterixException {
        int s = offset;
        int openPartOffset = 0;
        boolean isExpanded = false;

        int numberOfSchemaFields = 0;

        // Tag mark
        // 1 byte - Record tag, 24 (excluded when type is known)
        int i = setMark(0, ByteAnnotationField.TAG, s);

        // Length
        // 4 bytes - Total number of bytes
        i = setMark(i, ByteAnnotationField.LENGTH, s + 1);

        if (inputRecType == null) {
            openPartOffset = s + AInt32SerializerDeserializer.getInt(bytes, s + 6);
            i = setMark(i, ByteAnnotationField.OPENPART_OFFSET, openPartOffset);
            s += 8;
            isExpanded = true;
        } else {
            numberOfSchemaFields = inputRecType.getFieldNames().length;
            if (inputRecType.isOpen()) {
                i = setMark(i, ByteAnnotationField.IS_EXPANDED, s + 5);
                isExpanded = bytes[s + 5] == 1 ? true : false;
                if (isExpanded) {
                    openPartOffset = s + AInt32SerializerDeserializer.getInt(bytes, s + 6);
                    i = setMark(i, ByteAnnotationField.OPENPART_OFFSET, s + 6);
                    s += 10;
                } else {
                    s += 6;
                }
            } else {
                s += 5;
            }
        }

        //4 bytes - Number of closed fields
        i = setMark(i, ByteAnnotationField.NUMBER_CLOSED_FIELDS, s);
        s += 4;

        if (numberOfSchemaFields > 0) {
            boolean hasNullableFields = NonTaggedFormatUtil.hasNullableField(inputRecType);
            int nullBitMapLength = 0;
            if (hasNullableFields) {
                // Nullbitmap bytes
                i = setMark(i, ByteAnnotationField.NULLBITMAP, s);
                nullBitMapLength = (numberOfSchemaFields % 8 == 0 ? numberOfSchemaFields / 8
                        : numberOfSchemaFields / 8 + 1);
            }

            // Offset of the closed field values
            s += nullBitMapLength;
            i = setMark(i, ByteAnnotationField.CLOSED_FIELD_OFFSETS, s);

            // Offset of the closed field values
            s += 4 * numberOfSchemaFields;
            i = setMark(i, ByteAnnotationField.CLOSED_FIELDS, s);
        }

        if (isExpanded) {
            int numberOfOpenFields = AInt32SerializerDeserializer.getInt(bytes, openPartOffset);
            //  4 bytes - Number of open fields
            s = openPartOffset;
            i = setMark(i, ByteAnnotationField.NUMBER_OF_OPEN_FIELDS, s);
            s += 4;

            // 4 bytes openfield offsets and 4 Hash code for each field
            i = setMark(i, ByteAnnotationField.OPEN_FIELDS_HASH_OFFSET, s);
            s += 8 * numberOfOpenFields;

            // Open fields
            i = setMark(i, ByteAnnotationField.OPEN_FIELDS, s);
        }

        return i;
    }

    private int setMark(int index, ByteAnnotationField field, int offset) {
        markNames[index] = field.fieldName();
        marks[index] = offset;
        return index + 1;
    }
}
