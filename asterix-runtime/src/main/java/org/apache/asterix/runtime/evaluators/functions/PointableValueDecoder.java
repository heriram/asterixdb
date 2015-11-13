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

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.asterix.om.util.ResettableByteArrayOutputStream;
import org.apache.asterix.om.util.admdebugger.FieldTypeComputerUtils.FlatField;
import org.apache.asterix.om.util.admdebugger.FieldTypeComputerUtils.ListField;
import org.apache.asterix.om.util.admdebugger.FieldTypeComputerUtils.RecordField;
import org.apache.asterix.om.util.admdebugger.IAdmBytesField;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class PointableValueDecoder {

    private final static ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ASTRING);

    private final PointableAllocator pa = new PointableAllocator();

    private PointableUtils pu;

    private final int MAX_NUMBER_OF_MARKS = 12; // Max number of marks in a record + 1 for the end mark
    private final int[] marks = new int[MAX_NUMBER_OF_MARKS];
    private final String[] markNames = new String[MAX_NUMBER_OF_MARKS];

    private final AMutableString fieldNameHolder = new AMutableString("");

    private final StringBuilder valueHolder = new StringBuilder();
    private ArrayBackedValueStorage valueBuffer = new ArrayBackedValueStorage();

    private final ResettableByteArrayOutputStream outputBos = new ResettableByteArrayOutputStream();
    private final DataOutputStream outputDos = new DataOutputStream(outputBos);
    private final IVisitablePointable naReference = pa.allocateEmpty();


    public PointableValueDecoder(PointableUtils pu) {
        this.pu = pu;
        try {
            outputBos.reset();
            int start = outputBos.size();
            outputDos.writeByte(ATypeTag.STRING.serialize());
            outputDos.writeUTF("N/A");
            int end = outputBos.size();
            naReference.set(outputBos.getByteArray(), start, end - start);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public ARecordVisitablePointable getAnnotatedByteArray(IVisitablePointable valuePointable)
            throws IOException, AsterixException {
        return getAnnotatedByteArray(valuePointable, DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
    }

    public ARecordVisitablePointable getAnnotatedByteArray(IVisitablePointable valuePointable, ARecordType requiredRecType)
            throws IOException, AsterixException {
        ARecordVisitablePointable annotRecord = (ARecordVisitablePointable) pa.allocateRecordValue(requiredRecType);
        getAnnotatedByteArray(valuePointable, requiredRecType, annotRecord);
        return annotRecord;
    }

    public void getAnnotatedByteArray(IVisitablePointable valuePointable, IVisitablePointable annotRecord)
            throws IOException, AsterixException {
        getAnnotatedByteArray(valuePointable, DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, annotRecord);
    }

    public void getAnnotatedByteArray(IVisitablePointable valuePointable, ARecordType requiredRecType,
            IVisitablePointable annotRecord) throws IOException, AsterixException {
        int markCount = decodeValue(valuePointable);
        byte[] bytes = valuePointable.getByteArray();

        IARecordBuilder recordBuilder = pu.getRecordBuilder();
        recordBuilder.reset(requiredRecType);
        recordBuilder.init();

        ArrayBackedValueStorage buffer = pu.getTempBuffer();
        IVisitablePointable byteArrayValue = pa.allocateEmpty();
        int numfields = requiredRecType.getFieldTypes().length;
        boolean nullFields[] = new boolean[numfields];
        Arrays.fill(nullFields, true);
        for (int i=0; i<markCount; i++) {
            int id = requiredRecType.findFieldPosition(markNames[i]);
            int offset = marks[i];
            int length = marks[i + 1] - marks[i];
            setByteArrayPointableValue(bytes, offset, length, byteArrayValue);
            if (id>-1) {
                nullFields[id] = false;
                recordBuilder.addField(id, byteArrayValue);
            } else {
                fieldNameHolder.setValue(markNames[i]);
                buffer.reset();
                stringSerde.serialize(fieldNameHolder, buffer.getDataOutput());
                recordBuilder.addField(buffer, byteArrayValue);
            }
        }

        // Deal with not applicable fields (if enabled)
        if (!requiredRecType.isOpen()) {
            for (int i = 0; i < numfields; i++) {
                if (nullFields[i])
                    recordBuilder.addField(i, naReference);
            }
        }
        buffer.reset();
        recordBuilder.write(buffer.getDataOutput(), true);

        annotRecord.set(buffer.getByteArray(), buffer.getStartOffset(), buffer.getLength());
    }

    public void setByteArrayPointableValue(byte bytes[], int offset, int length, IVisitablePointable vp) throws IOException {
        valueHolder.setLength(0);
        valueHolder.append("[" + bytes[offset]);
        int end = offset + length;
        for (int i=offset+1; i<end; i++) {
            valueHolder.append(", " + bytes[i]);
        }
        valueHolder.append("]");

        DataOutput out = valueBuffer.getDataOutput();
        valueBuffer.reset();
        out.writeByte(ATypeTag.STRING.serialize());
        out.writeUTF(valueHolder.toString());
        vp.set(valueBuffer.getByteArray(), valueBuffer.getStartOffset(), valueBuffer.getLength());
    }

    private int decodeValue(IVisitablePointable valuePointable)
            throws HyracksDataException, AsterixException {
        byte[] bytes = valuePointable.getByteArray();
        int s = valuePointable.getStartOffset();
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[s]);

        int markCount = 0;
        switch (typeTag) {
            case RECORD:
                markCount = decodeRecord(bytes, s, ((ARecordVisitablePointable)valuePointable).getInputRecordType());
                break;
            case UNORDEREDLIST:
            case ORDEREDLIST:
                markCount = decodeList(bytes, s);
                break;
            case STRING:
            case UUID_STRING:
                markCount = setMark(0, FlatField.TAG, s);
                // 2 bytes string length
                setMark(markCount, FlatField.LENGTH, s+1);
                // Avoid showing wrong values of empty strings
                if (valuePointable.getLength()>3)
                    setMark(markCount, FlatField.VALUE, s+3);
                break;
            default:
                markCount = setMark(0, FlatField.TAG, s);
                markCount = setMark(markCount, FlatField.VALUE, s+1);

        }
        // Set an end mark
        marks[markCount] = s + valuePointable.getLength();

        return markCount;
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
    private int decodeList(byte bytes[], int offset)
            throws HyracksDataException, AsterixException {
        int s = offset;
        int i = setMark(0, ListField.TAG, s);
        s++;
        // 1 byte - Type of items on the list
        i = setMark(i, ListField.ITEM_TYPE, s);
        s++;
        //4 bytes - Total number of bytes
        i = setMark(i, ListField.LENGTH, s);
        s+=4;
        //4 bytes - Number of items
        i = setMark(i, ListField.NUMBER_OF_ITEMS, s);
        int numberOfItems = AInt32SerializerDeserializer.getInt(bytes, s);
        s+=4;
        // 4 bytes for each item
        i= setMark(i, ListField.ITEM_OFFSETS, s);
        s += 4 * numberOfItems;
        // The rest is the values of the items
        return setMark(i, ListField.VALUE, s);
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
    private int decodeRecord(byte bytes[], int offset, ARecordType inputRecType)
            throws HyracksDataException, AsterixException {
        int s = offset;
        int openPartOffset = 0;
        boolean isExpanded = false;

        int numberOfSchemaFields = 0;

        // Tag mark
        // 1 byte - Record tag, 24 (excluded when type is known)
        int i = setMark(0, RecordField.TAG, s);

        // Length
        // 4 bytes - Total number of bytes
        i = setMark(i, RecordField.LENGTH, s+1);

        if (inputRecType == null) {
            openPartOffset = s + AInt32SerializerDeserializer.getInt(bytes, s + 6);
            i = setMark(i, RecordField.OPENPART_OFFSET, openPartOffset);
            s += 8;
            isExpanded = true;
        } else {
            numberOfSchemaFields = inputRecType.getFieldNames().length;
            if (inputRecType.isOpen()) {
                i = setMark(i, RecordField.IS_EXPANDED, s+5);
                isExpanded = bytes[s + 5] == 1 ? true : false;
                if (isExpanded) {
                    openPartOffset = s + AInt32SerializerDeserializer.getInt(bytes, s + 6);
                    i = setMark(i, RecordField.OPENPART_OFFSET, s + 6);
                    s += 10;
                } else {
                    s += 6;
                }
            } else {
                s += 5;
            }
        }

        //4 bytes - Number of closed fields
        i = setMark(i, RecordField.NUMBER_CLOSED_FIELDS, s);
        s+=4;

        if (numberOfSchemaFields > 0) {
            boolean hasNullableFields = NonTaggedFormatUtil.hasNullableField(inputRecType);
            int nullBitMapLength = 0;
            if (hasNullableFields) {
                // Nullbitmap bytes
                i = setMark(i, RecordField.NULLBITMAP, s);
                nullBitMapLength = (numberOfSchemaFields % 8 == 0 ? numberOfSchemaFields / 8
                        : numberOfSchemaFields / 8 + 1);
            }

            // Offset of the closed field values
            s += nullBitMapLength;
            i = setMark(i, RecordField.CLOSED_FIELD_OFFSETS, s);

            // Offset of the closed field values
            s += 4*numberOfSchemaFields;
            i = setMark(i, RecordField.CLOSED_FIELDS, s);
        }

        if (isExpanded) {
            int numberOfOpenFields = AInt32SerializerDeserializer.getInt(bytes, openPartOffset);
            //  4 bytes - Number of open fields
            s = openPartOffset;
            i = setMark(i, RecordField.NUMBER_OF_OPEN_FIELDS, s);
            s+=4;

            // 4 bytes openfield offsets and 4 Hash code for each field
            i = setMark(i, RecordField.OPEN_FIELDS_HASH_OFFSET, s);
            s += 8 * numberOfOpenFields;

            // Open fields
            i = setMark(i, RecordField.OPEN_FIELDS, s);
        }

        return i;
    }

    private int setMark(int index, IAdmBytesField field, int offset) {
        markNames[index] = field.fieldName();
        marks[index] = offset;
        return index+1;
    }
}
