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

import org.apache.asterix.builders.AbstractListBuilder;
import org.apache.asterix.builders.AbvsBuilderFactory;
import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.ListBuilderFactory;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilderFactory;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.typecomputer.impl.AnnotatedBytesTypeComputer;
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
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PrintAdmBytesHelper {
    private IObjectPool<IARecordBuilder, ATypeTag> recordBuilderPool = new ListObjectPool<IARecordBuilder, ATypeTag>(
            new RecordBuilderFactory());
    private IObjectPool<IAsterixListBuilder, ATypeTag> listBuilderPool = new ListObjectPool<IAsterixListBuilder, ATypeTag>(
            new ListBuilderFactory());
    private IObjectPool<IMutableValueStorage, ATypeTag> abvsBuilderPool = new ListObjectPool<IMutableValueStorage, ATypeTag>(
            new AbvsBuilderFactory());

    private final static ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ASTRING);

    public static PrintAdmBytesHelper getInstance() {
        return new PrintAdmBytesHelper();
    }
    public static PrintAdmBytesHelper INSTANCE = new PrintAdmBytesHelper();


    private final PointableAllocator pa = new PointableAllocator();
    private final ArrayBackedValueStorage tempBuffer = new ArrayBackedValueStorage();

    private final ArrayBackedValueStorage tempBuffer0 = new ArrayBackedValueStorage();
    private final ArrayBackedValueStorage tempBuffer1 = new ArrayBackedValueStorage();

    private static final String TAG = "tag";
    private static final String LENGTH = "length";
    private static final String VALUE = "value";
    private static final Map<String, Integer> annotationRecordFields = new HashMap<String, Integer>();
    private final static AString tagName = new AString(TAG);
    private final static AString lengthName = new AString(LENGTH);
    private final static AString valueName = new AString(VALUE);
    public static final String PRINT_FIELD_NAMES[] = {TAG, LENGTH, VALUE};
    public static final IAType PRINT_BYTE_ARRAY[] = { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING};

    private final ARecordType fieldRecordType = AnnotatedBytesTypeComputer.annotatedBytesType;


    private PrintAdmBytesHelper(){
    }

    public void reset() {
        abvsBuilderPool.reset();
        recordBuilderPool.reset();
        listBuilderPool.reset();
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

    public ARecordType loadRequireType(ARecordType inputRecordType, long level, long maxLevel) throws AlgebricksException {
        if (level<=1 || level==maxLevel) {
            return getFieldRecordType();
        }

        String fieldNames[] = inputRecordType.getFieldNames();
        IAType fieldTypes[] = inputRecordType.getFieldTypes();

        IAType[] newTypes = new IAType[fieldNames.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            if (fieldTypes[i].getTypeTag() == ATypeTag.RECORD) {
                newTypes[i] = loadRequireType((ARecordType) fieldTypes[i], level + 1, maxLevel);
            } else {
                newTypes[i] = getFieldRecordType();
            }
        }
        try {
            String typeName = "annotated(" + inputRecordType.getTypeName() + ")";
            return new ARecordType(typeName, fieldNames, newTypes, inputRecordType.isOpen());
        } catch (AsterixException | HyracksException e) {
            throw new AlgebricksException(e);
        }
    }

    /**
     * Print annotated byte array as a record: {"tag":"[13]", "length": "[,...,]", "value":"[,..,]" }
     *
     * //@param outputType
     * @param vr
     * @param out
     * @throws IOException
     * @throws AsterixException
     * @throws AlgebricksException
     */
    public void printAnnotatedBytes(IValueReference vr, DataOutput out)
            throws IOException, AsterixException {
        byte[] bytes = vr.getByteArray();
        int offset = vr.getStartOffset();
        int valueOffset = offset + PrintAdmBytesHelper.getValueOffset(bytes[offset]);
        int valueLength = vr.getLength() - valueOffset + offset; // value length
        int lengthBytesLength = valueOffset - offset - 1; // value length

        AString tagByteValue = new AString(byteArrayToString(bytes, 0, 1, false));
        AString lengthBytesValue = new AString(byteArrayToString(bytes, offset + 1, lengthBytesLength, false));
        AString valueBytesValue = new AString(byteArrayToString(bytes, valueOffset, valueLength, false));

        IARecordBuilder recordBuilder = getRecordBuilder();
        recordBuilder.init();
        recordBuilder.reset(fieldRecordType);

        addField(tagName, tagByteValue, recordBuilder);
        addField(lengthName, lengthBytesValue, recordBuilder);
        addField(valueName, valueBytesValue, recordBuilder);

        recordBuilder.write(out, true);
    }

    public void printAdmBytes(IValueReference vr, DataOutput output)
            throws AlgebricksException {
        IARecordBuilder recordBuilder = getRecordBuilder();
        recordBuilder.init();
        recordBuilder.reset(fieldRecordType);

        printAdmBytes(vr, recordBuilder);
        try {
            recordBuilder.write(output, true);
        } catch (IOException | AsterixException e) {
            throw new AlgebricksException("Error generating field value (annotated)");
        }
    }

    public void printAdmBytes(IValueReference vr, IARecordBuilder recordBuilder)
            throws AlgebricksException {

        byte[] bytes = vr.getByteArray();
        int offset = vr.getStartOffset();
        int valueOffset = offset + PrintAdmBytesHelper.getValueOffset(bytes[offset]);
        int valueLength = vr.getLength() - valueOffset + offset; // value length
        int lengthBytesLength = valueOffset - offset - 1; // value length

        try {
            recordBuilder.init();
            recordBuilder.reset(fieldRecordType);

            addAnnotatedField(recordBuilder, TAG, bytes, 0, 1);
            addAnnotatedField(recordBuilder, LENGTH, bytes, offset + 1, lengthBytesLength);
            addAnnotatedField(recordBuilder, VALUE, bytes, valueOffset, valueLength);

        } catch (IOException e) {
            throw new AlgebricksException("Error generating field value (annotated)");
        }

    }

    private void addAnnotatedField(IARecordBuilder recordBuilder, String fieldName,
            byte byteArray[], int offset, int length) throws HyracksDataException {
        tempBuffer1.reset();
        stringSerde.serialize(new AString(byteArrayToString(byteArray, offset, length, false)),
                tempBuffer1.getDataOutput());
        int pos = recordBuilder.getFieldId(fieldName);
        recordBuilder.addField(pos, tempBuffer1);
    }


    public void printAnnotatedBytes(IVisitablePointable inputVp, IVisitablePointable outputVp)
            throws AlgebricksException {
        tempBuffer.reset();
        try {
            printAnnotatedBytes(inputVp, tempBuffer.getDataOutput());
            outputVp.set(tempBuffer.getByteArray(), tempBuffer.getStartOffset(), tempBuffer.getLength());
        } catch (IOException | AsterixException e) {
           throw new AlgebricksException("Could not print annotated bytes");
        }

    }

    public void addField(AString fname, AString fvalue, IARecordBuilder recordBuilder)
            throws HyracksDataException, AsterixException {

        ArrayBackedValueStorage valueAbvs = new ArrayBackedValueStorage();
        IVisitablePointable valuePointableValue = pa.allocateFieldValue(BuiltinType.ASTRING);
        valueAbvs.reset();
        stringSerde.serialize(fvalue, valueAbvs.getDataOutput());
        valuePointableValue.set(valueAbvs);

        int pos = recordBuilder.getFieldId(fname.getStringValue());
        if (pos>-1) {
            recordBuilder.addField(pos, valuePointableValue);
        } else {
            ArrayBackedValueStorage fieldAbvs = new ArrayBackedValueStorage();
            fieldAbvs.reset();
            IVisitablePointable fieldPointableValue = pa.allocateFieldValue(BuiltinType.ASTRING);
            stringSerde.serialize(fname, fieldAbvs.getDataOutput());
            fieldPointableValue.set(fieldAbvs);
            recordBuilder.addField(fieldPointableValue, valuePointableValue);
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

    public IARecordBuilder getRecordBuilder() {
        return recordBuilderPool.allocate(ATypeTag.RECORD);
    }

    public AbstractListBuilder getListBuilder(ATypeTag listType) {
        if (listType==ATypeTag.UNORDEREDLIST)
            return (UnorderedListBuilder) listBuilderPool.allocate(ATypeTag.UNORDEREDLIST);
        else
            return (OrderedListBuilder) listBuilderPool.allocate(ATypeTag.ORDEREDLIST);
    }
}
