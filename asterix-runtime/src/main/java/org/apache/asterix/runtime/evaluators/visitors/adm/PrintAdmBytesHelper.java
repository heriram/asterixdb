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
package org.apache.asterix.runtime.evaluators.visitors.adm;

import org.apache.asterix.builders.AbvsBuilderFactory;
import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.ListBuilderFactory;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.builders.RecordBuilderFactory;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.nonvisitor.AListPointable;
import org.apache.asterix.om.pointables.nonvisitor.ARecordPointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

import java.io.DataOutput;
import java.io.IOException;

public class PrintAdmBytesHelper {
    private IObjectPool<IARecordBuilder, ATypeTag> recordBuilderPool = new ListObjectPool<IARecordBuilder, ATypeTag>(
            new RecordBuilderFactory());
    private IObjectPool<IAsterixListBuilder, ATypeTag> listBuilderPool = new ListObjectPool<IAsterixListBuilder, ATypeTag>(
            new ListBuilderFactory());
    private IObjectPool<IMutableValueStorage, ATypeTag> abvsBuilderPool = new ListObjectPool<IMutableValueStorage, ATypeTag>(
            new AbvsBuilderFactory());
    private IObjectPool<IPointable, ATypeTag> recordPointablePool = new ListObjectPool<IPointable, ATypeTag>(
            ARecordPointable.ALLOCATOR);
    private IObjectPool<IPointable, ATypeTag> listPointablePool = new ListObjectPool<IPointable, ATypeTag>(
            AListPointable.ALLOCATOR);

    public static PrintAdmBytesHelper getInstance() {
        return new PrintAdmBytesHelper();
    }

    private AMutableString valueString = new AMutableString("");
    private final RecordBuilder recordBuilder = new RecordBuilder();
    private ARecordType fieldRecordType;

    private final int TAG_ID=0, LENGTH_ID=1, VALUE_ID=2;


    private PrintAdmBytesHelper(){
        String arfName[] = new String[]{"tag", "length", "value"};
        IAType arfByteArray[] = new IAType[]{ BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING};
        try {
            fieldRecordType = new ARecordType("byteArrayfield", arfName, arfByteArray, true);
        } catch (AsterixException | HyracksDataException e) {
            e.printStackTrace();
        }
    }

    public void reset() {
        abvsBuilderPool.reset();
    }

    public String byteArrayToString(byte bytes[], int offset, int length) {
        StringBuilder sb = new StringBuilder("[");
        sb.append(bytes[offset] & 0xff);
        int end = offset + length;
        for (int i=offset+1; i<end; i++) {
            sb.append(", ");
            sb.append(bytes[i] & 0xff);

        }
        sb.append(']');

        return sb.toString();
    }

    public ARecordType getFieldRecordType() {
        return fieldRecordType;
    }

    public void printAnnotatedBytes(IVisitablePointable vp, DataOutput out) throws IOException, AsterixException {
        byte[] bytes = vp.getByteArray();
        int startOffset = vp.getStartOffset();
        int valueStartOffset = startOffset + getValueOffset(bytes[startOffset]);
        int len = vp.getLength() - valueStartOffset + startOffset; // value length

        recordBuilder.init();
        recordBuilder.reset(fieldRecordType);
        recordBuilder.addField(TAG_ID, byteArrayToStringPointable(new byte[] { bytes[startOffset] }, 0, 1));

        IValueReference lengthVr = extractLengthString(bytes, startOffset+1, valueStartOffset);
        if (lengthVr != null)
            recordBuilder.addField(LENGTH_ID, lengthVr);
        recordBuilder.addField(VALUE_ID, byteArrayToStringPointable(bytes, valueStartOffset, len));

        recordBuilder.write(out, true);
    }

    private IValueReference byteArrayToStringPointable(byte[] bytes, int offset, int length) throws HyracksDataException {
        //AString valueString = new AString(byteArrayToString(bytes, offset, length));
        valueString.setValue(byteArrayToString(bytes, offset, length));
        ArrayBackedValueStorage tempBuffer = getTempBuffer();
        tempBuffer.reset();
        AStringSerializerDeserializer.INSTANCE.serialize(valueString, tempBuffer.getDataOutput());

        return tempBuffer;
    }

   /* private IValueReference getByteArrayAsStringPointable(byte byteArray[], int offset, int length) throws IOException {
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
    }*/


    private IValueReference extractLengthString(byte[] vpBytes, int offset, int valueOffset)
            throws HyracksDataException {
        if (valueOffset>1) {
            return byteArrayToStringPointable(vpBytes, offset, valueOffset - offset);
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

    public ArrayBackedValueStorage getTempBuffer() {
        return (ArrayBackedValueStorage) abvsBuilderPool.allocate(ATypeTag.BINARY);
    }

    public ARecordPointable getRecordPointable() {
        return (ARecordPointable) recordPointablePool.allocate(ATypeTag.RECORD);
    }

    public AListPointable getListPointable() {
        return (AListPointable) listPointablePool.allocate(ATypeTag.ORDEREDLIST);
    }

    public IARecordBuilder getRecordBuilder() {
        return recordBuilderPool.allocate(ATypeTag.RECORD);
    }

    public OrderedListBuilder getOrderedListBuilder() {
        return (OrderedListBuilder) listBuilderPool.allocate(ATypeTag.ORDEREDLIST);
    }
}
