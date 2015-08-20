/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.runtime.evaluators.functions;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.pointables.ARecordVisitablePointable;
import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.api.IMutableValueStorage;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.api.IValueReference;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * An utility class for some frequently used methods like checking the equality between two pointables (binary values)
 * (e.g., field names), string value of a fieldname pointable, getting the typetag of a pointable, etc.
 *
 * Note: To get the typetag of a fieldvalue (i) in a record, it is recommended to use the getFieldTypeTags().get(i)
 * method rather than getting it from fhe field value itself.
 *
 */

public class PointableUtils {

    static final ByteArrayAccessibleOutputStream outputStream = new ByteArrayAccessibleOutputStream();
    static final DataInputStream dis = new DataInputStream(
            new ByteArrayInputStream(outputStream.getByteArray()));

    private static final byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();

    @SuppressWarnings("unchecked")
    public static final ISerializerDeserializer<ANull> NULL_SERDE = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ANULL);

    private static final IBinaryComparator STRING_BINARY_COMPARATOR = PointableBinaryComparatorFactory
            .of(UTF8StringPointable.FACTORY).createBinaryComparator();

    public static final PointableUtils INSTANCE = new PointableUtils();

    private PointableUtils(){
    }

    public static int compareStringBinValues(IValueReference a, IValueReference b) throws HyracksDataException {
        // start+1 and len-1 due to type tag ignore (only interested in String value)
        return STRING_BINARY_COMPARATOR.compare(a.getByteArray(), a.getStartOffset() + 1, a.getLength() - 1,
                b.getByteArray(), b.getStartOffset() + 1, b.getLength() - 1);
    }

    public static boolean isEqual(IValueReference a, IValueReference b) throws HyracksDataException {
        return (compareStringBinValues(a, b)==0);
    }

    public static boolean byteArrayEqual(IValueReference valueRef1, IValueReference valueRef2)
            throws HyracksDataException {
        return byteArrayEqual(valueRef1, valueRef2, 3);
    }

    public static boolean byteArrayEqual(IValueReference valueRef1, IValueReference valueRef2, int dataOffset)
            throws HyracksDataException {
        if (valueRef1 == null || valueRef2 == null) return false;
        if (valueRef1 == valueRef2) return true;

        int length1 = valueRef1.getLength();
        int length2 = valueRef2.getLength();

        if (length1 != length2) return false;

        byte[] bytes1 = valueRef1.getByteArray();
        byte[] bytes2 = valueRef2.getByteArray();
        int start1 = valueRef1.getStartOffset() + dataOffset;
        int start2 = valueRef2.getStartOffset() + dataOffset;

        int end = start1+length1-dataOffset;

        for (int i=start1, j=start2; i<end; i++,j++) {
            if (bytes1[i] != bytes2[j]) return false;
        }

        return true;
    }

    public String getFieldName(IValueReference fieldNamePointable) throws IOException {
        outputStream.reset();
        outputStream.write(fieldNamePointable.getByteArray(),
                fieldNamePointable.getStartOffset() + 1, fieldNamePointable.getLength());
        dis.reset();

        return AStringSerializerDeserializer.INSTANCE.deserialize(dis).getStringValue();
    }

    public static boolean isType(ATypeTag typeTag, IVisitablePointable visitablePointable) {
        return (getTypeTag(visitablePointable)==typeTag);
    }

    public static ATypeTag getTypeTag(IVisitablePointable visitablePointable) {
        byte[] bytes = visitablePointable.getByteArray();
        int s = visitablePointable.getStartOffset();
        return EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[s]);
    }


    public boolean isNullRecord(IMutableValueStorage abvs, DataOutput output) throws AlgebricksException {
        if (abvs.getByteArray()[0] == SER_NULL_TYPE_TAG) {
            try {
                NULL_SERDE.serialize(ANull.NULL, output);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
            }
            return true;
        }

        return false;
    }

    public static boolean isAFieldName(ARecordVisitablePointable recordPointer, IVisitablePointable fieldNamePointable) {
        int fieldPosition = getFieldNamePosition(recordPointer, fieldNamePointable);

        return (fieldPosition>-1);
    }

    public static int getFieldNamePosition(ARecordVisitablePointable recordPointer, IVisitablePointable fieldNamePointable) {
        for (int i = 0; i < recordPointer.getFieldNames().size(); ++i) {
            IVisitablePointable fp = recordPointer.getFieldNames().get(i);
            if (fp.equals(fieldNamePointable)) {
                return i;
            }
        }
        return -1;
    }

    /**
     *
     * @param str
     *    The input string
     * @param vs
     *    The storage buffer
     * @param fnp
     *    The pointable, e.g., fnp = UTF8StringPointable.FACTORY.createPointable();
     *
     * @throws AlgebricksException
     */
    public static void serializeString(String str, IMutableValueStorage vs, IPointable fnp) throws AlgebricksException {
        vs.reset();
        try {
            UTF8StringSerializerDeserializer.INSTANCE.serialize(str, vs.getDataOutput());
        } catch (HyracksDataException e) {
            throw new AlgebricksException("Could not serialize " + str);
        }
        fnp.set(vs);
    }
}
