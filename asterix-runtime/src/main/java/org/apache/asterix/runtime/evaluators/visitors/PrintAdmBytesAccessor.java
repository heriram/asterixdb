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

package org.apache.asterix.runtime.evaluators.visitors;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.om.base.AMutableInt16;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.functions.PointableUtils;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

class PrintAdmBytesAccessor {
    private final IAType BYTE_ARRAY_TYPE = new AOrderedListType(BuiltinType.AINT16, null);
    private final RecordBuilder fieldRecordBuild = new RecordBuilder();
    private final OrderedListBuilder fieldListBuilder = new OrderedListBuilder();
    private ARecordType fieldRecordType;

    private final int TAG_ID=0, LENGTH_ID=1, VALUE_ID=3;
    private final AMutableString byteArrayString = new AMutableString("");

    private final PrintAdmBytesHelper printHelper = PrintAdmBytesHelper.getInstance();

    private PrintAdmBytesVisitor visitor;

    public PrintAdmBytesAccessor() {
        String arfName[] = new String[]{"tag", "length", "value"};
        IAType arfByteArray[] = new IAType[]{BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING};
        try {
            fieldRecordType = new ARecordType("byteArrayfield", arfName, arfByteArray, true);
        } catch (AsterixException| HyracksDataException e) {
            e.printStackTrace();
        }

    }

    public PrintAdmBytesHelper getPrintHelper() {
        return printHelper;
    }

    public String accessRecord(IVisitablePointable recAccessor, PrintAdmBytesVisitor visitor, long maxLevel,
            Triple<IVisitablePointable, RecordBuilder, Long> arg)
            throws AsterixException, IOException {

        ARecordVisitablePointable rec = ((ARecordVisitablePointable) recAccessor);
        List<IVisitablePointable> fieldNames = rec.getFieldNames();
        List<IVisitablePointable> fieldValues = rec.getFieldValues();
        List<IVisitablePointable> fieldTypeTags = rec.getFieldTypeTags();

        printHelper.reset();

        ArrayBackedValueStorage tempBuffer = printHelper.getTempBuffer();
        for (int i=0; i<fieldNames.size(); i++) {
            ATypeTag typeTag = PointableUtils.getTypeTag(fieldTypeTags.get(i));
            arg.first = fieldNames.get(i);
            switch (typeTag){
                case RECORD:
                    arg.third++; // Increase the current level
                    if (arg.third <= maxLevel) {
                        ((ARecordVisitablePointable) fieldValues.get(i)).accept(visitor, arg);
                    } else {
                        printAnnotatedBytes(fieldValues.get(i), tempBuffer.getDataOutput());
                        arg.second.addField(fieldNames.get(i), tempBuffer);
                    }
                    break;
                case ORDEREDLIST:
                case UNORDEREDLIST:
                    ((AListVisitablePointable)fieldValues.get(i)).accept(visitor, arg);
                    break;
                default:
                    ((AFlatValuePointable)fieldValues.get(i)).accept(visitor, arg);
            }
        }

        return arg.second.toString();
    }


    public void printAnnotatedBytes(IVisitablePointable vp, DataOutput out) throws IOException, AsterixException {
        byte[] bytes = vp.getByteArray();
        int startOffset = vp.getStartOffset();
        int valueStartOffset = startOffset + getValueOffset(bytes[startOffset]);
        int len = vp.getLength() - valueStartOffset + startOffset; // value length

        fieldRecordBuild.reset(fieldRecordType);
        fieldRecordBuild.addField(TAG_ID, byteArrayToString(new byte[] { bytes[startOffset] }, 0, 1));

        IValueReference lengthVr = extractLengthString(bytes, startOffset+1, valueStartOffset);
        if (lengthVr != null)
            fieldRecordBuild.addField(LENGTH_ID, lengthVr);
        fieldRecordBuild.addField(VALUE_ID, byteArrayToString(bytes, valueStartOffset, len));

        fieldRecordBuild.write(out, true);
    }


    private IValueReference byteArrayToString(byte[] bytes, int offset, int length) throws HyracksDataException {
        StringBuilder sb = new StringBuilder("[");
        sb.append(bytes[offset] & 0xff);
        int end = offset + length;
        for (int i=offset+1; i<end; i++) {
            sb.append(", ");
            sb.append(bytes[i] & 0xff);

        }
        sb.append(']');

        byteArrayString.setValue(sb.toString());
        ArrayBackedValueStorage tempBuffer = printHelper.getTempBuffer();
        AStringSerializerDeserializer.INSTANCE.serialize(byteArrayString, tempBuffer.getDataOutput());
        return tempBuffer;
    }

    private IValueReference getByteArrayAsStringPointable(byte byteArray[], int offset, int length) throws IOException {
        fieldListBuilder.reset((AbstractCollectionType) BYTE_ARRAY_TYPE);
        AMutableInt16 int16 = new AMutableInt16((short)0);

        ArrayBackedValueStorage tempBuffer = printHelper.getTempBuffer();
        for(int i=offset; i<length; i++) {
            tempBuffer.reset();
            int16.setValue((short)byteArray[i]);
            AInt16SerializerDeserializer.INSTANCE.serialize(int16, tempBuffer.getDataOutput());
            fieldListBuilder.addItem(tempBuffer);
        }
        tempBuffer.reset();
        fieldListBuilder.write(tempBuffer.getDataOutput(), true);
        return tempBuffer;
    }


    private IValueReference extractLengthString(byte[] vpBytes, int offset, int valueOffset)
            throws HyracksDataException {
        if (valueOffset>1) {
            return byteArrayToString(vpBytes, offset, valueOffset - offset);
        }
        return null;
    }

    // Get the relative value offset
    private int getValueOffset(byte typeByte) {
        int length=0;
        switch (EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(typeByte)) {
            case STRING:
            case UUID_STRING:
                return 3;
            case ORDEREDLIST:
            case UNORDEREDLIST:
                return 10;
            case RECORD:
            case SPARSERECORD:
                return 5;
            default:
                return 1;
        }
    }


}
