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
import org.apache.asterix.builders.RecordBuilderFactory;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.pointables.PointableAllocator;
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
import org.apache.asterix.runtime.evaluators.functions.PointableUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IPointable;
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

    private final static ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ASTRING);

    public static PrintAdmBytesHelper getInstance() {
        return new PrintAdmBytesHelper();
    }

    private final PointableAllocator pa = new PointableAllocator();
    private final ArrayBackedValueStorage tempBuffer = new ArrayBackedValueStorage();

    private final int TAG_ID=0, LENGTH_ID=1, VALUE_ID=2;
    public static final String PRINT_FIELD_NAMES[] = {"tag", "length", "value"};
    public static final IAType PRINT_BYTE_ARRAY[] = { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING};
    public static ARecordType fieldRecordType;
    static {
        try {
            fieldRecordType = new ARecordType("byteArrayfield", PRINT_FIELD_NAMES, PRINT_BYTE_ARRAY, true);
        } catch (AsterixException e) {
            e.printStackTrace();
        } catch (HyracksDataException e) {
            e.printStackTrace();
        }
    }

    private PrintAdmBytesHelper(){
    }

    public void reset() {
        abvsBuilderPool.reset();
    }

    public String byteArrayToString(byte bytes[], int offset, int length, boolean unsigned) {
        StringBuilder sb = new StringBuilder("[");

        int b = unsigned?(bytes[offset] & 0xff):bytes[offset];
        sb.append(b);
        int end = offset + length;
        for (int i=offset+1; i<end; i++) {
            sb.append(", ");
            b = unsigned?(bytes[i] & 0xff):bytes[i];
            sb.append(b);

        }
        sb.append(']');

        return sb.toString();
    }

    public ARecordType getFieldRecordType() {
        return fieldRecordType;
    }

    /**
     * Print annotated byte array as a record: {"tag":"[13]", "length": "[,...,]", "value":"[,..,]" }
     *
     * @param vp
     * @param out
     * @throws IOException
     * @throws AsterixException
     * @throws AlgebricksException
     */
    public void printAnnotatedBytes(IVisitablePointable vp, DataOutput out)
            throws IOException, AsterixException {
        byte[] bytes = vp.getByteArray();
        int startOffset = vp.getStartOffset();
        int valueStartOffset = startOffset + getValueOffset(bytes[startOffset]);
        int len = vp.getLength() - valueStartOffset + startOffset; // value length

        IARecordBuilder recordBuilder = getRecordBuilder();
        recordBuilder.init();
        recordBuilder.reset(fieldRecordType);

        try {
            IVisitablePointable idFieldValue = pa.allocateFieldValue(BuiltinType.ASTRING);

            byteArrayToStringPointable(new byte[] { bytes[startOffset] }, 0, 1, idFieldValue);

            recordBuilder.addField(TAG_ID, idFieldValue);

            if (valueStartOffset > 1) {
                IVisitablePointable lenFieldValue = pa.allocateFieldValue(BuiltinType.ASTRING);
                int vlen = valueStartOffset - startOffset - 1;
                byteArrayToStringPointable(bytes, startOffset + 1, vlen, lenFieldValue);
                recordBuilder.addField(LENGTH_ID, lenFieldValue);
            }

            IVisitablePointable valueFieldValue = pa.allocateFieldValue(BuiltinType.ASTRING);
            byteArrayToStringPointable(bytes, valueStartOffset, len, valueFieldValue);
            recordBuilder.addField(VALUE_ID, valueFieldValue);

            recordBuilder.write(out, true);

        } catch (AlgebricksException e) {
            new AsterixException("Error generating annotated bytes");
        }
    }

    private void byteArrayToStringPointable(byte bytes[], int offset, int length, IVisitablePointable vp)
            throws HyracksDataException, AlgebricksException {
        //ArrayBackedValueStorage tempBuffer = getTempBuffer();
        tempBuffer.reset();
        PointableUtils.serializeString(byteArrayToString(bytes, offset, length, false), tempBuffer, vp);
    }

    // Get the relative value offset
    public static int getValueOffset(byte typeByte) {
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
