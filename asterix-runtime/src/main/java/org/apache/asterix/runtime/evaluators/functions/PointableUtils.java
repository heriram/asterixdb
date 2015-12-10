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

import java.io.DataOutput;
import java.io.IOException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;

/**
 * An utility class for some frequently used methods like checking the equality between two pointables (binary values)
 * (e.g., field names), string value of a fieldname pointable, getting the typetag of a pointable, etc.
 * Note: To get the typetag of a fieldvalue (i) in a record, it is recommended to use the getFieldTypeTags().get(i)
 * method rather than getting it from fhe field value itself.
 */

public class PointableUtils {
    private static final IBinaryComparator STRING_BINARY_COMPARATOR = PointableBinaryComparatorFactory.of(
            UTF8StringPointable.FACTORY).createBinaryComparator();
    private final ISerializerDeserializer strSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ASTRING);
    private final AMutableString aString = new AMutableString("");

    public PointableUtils() {
    }

    public static int compareStringBinValues(IValueReference a, IValueReference b) throws HyracksDataException {
        // start+1 and len-1 due to type tag ignore (only interested in String value)
        return STRING_BINARY_COMPARATOR.compare(a.getByteArray(), a.getStartOffset() + 1, a.getLength() - 1,
                b.getByteArray(), b.getStartOffset() + 1, b.getLength() - 1);
    }

    public static boolean isEqual(IValueReference a, IValueReference b) throws HyracksDataException {
        return (compareStringBinValues(a, b) == 0);
    }

    public static boolean byteArrayEqual(IValueReference valueRef1, IValueReference valueRef2)
            throws HyracksDataException {
        return byteArrayEqual(valueRef1, valueRef2, 3);
    }

    public static boolean byteArrayEqual(IValueReference valueRef1, IValueReference valueRef2, int dataOffset)
            throws HyracksDataException {
        if (valueRef1 == null || valueRef2 == null) {
            return false;
        }
        if (valueRef1 == valueRef2) {
            return true;
        }

        int length1 = valueRef1.getLength();
        int length2 = valueRef2.getLength();

        if (length1 != length2) {
            return false;
        }

        byte[] bytes1 = valueRef1.getByteArray();
        byte[] bytes2 = valueRef2.getByteArray();
        int start1 = valueRef1.getStartOffset() + dataOffset;
        int start2 = valueRef2.getStartOffset() + dataOffset;

        int end = start1 + length1 - dataOffset;

        for (int i = start1, j = start2; i < end; i++, j++) {
            if (bytes1[i] != bytes2[j]) {
                return false;
            }
        }

        return true;
    }

    public static IVisitablePointable allocatePointable(PointableAllocator pa, IAType type) {
        if (type == null) {
            return pa.allocateEmpty();
        }

        switch (type.getTypeTag()) {
            case RECORD:
                return pa.allocateRecordValue(type);
            case ORDEREDLIST:
            case UNORDEREDLIST:
                return pa.allocateListValue(type);
            default:
                return pa.allocateFieldValue(type);
        }

    }

    public static long getLongValue(IValueReference valuePointable, boolean isTagged) throws AlgebricksException {
        ATypeTag tag = getTypeTag(valuePointable);

        int offset = isTagged ? valuePointable.getStartOffset() + 1 : valuePointable.getStartOffset();

        switch (tag) {
            case INT64:
                return LongPointable.getLong(valuePointable.getByteArray(), offset);
            case INT32:
                return IntegerPointable.getInteger(valuePointable.getByteArray(), offset);
            case INT16:
                return ShortPointable.getShort(valuePointable.getByteArray(), offset);
            default:
                throw new AlgebricksException("Unsupported type " + tag);
        }
    }

    public static boolean sameType(ATypeTag typeTag, IVisitablePointable visitablePointable) {
        return (getTypeTag(visitablePointable) == typeTag);
    }

    public static ATypeTag getTypeTag(IValueReference visitablePointable) {
        byte[] bytes = visitablePointable.getByteArray();
        int s = visitablePointable.getStartOffset();
        return EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[s]);
    }

    /**
     * @param str
     *            The input string
     * @param vs
     *            The storage buffer
     * @param writeTag
     *            Specifying whether a tag for the string should also be written
     * @throws AlgebricksException
     */
    public void serializeString(String str, IMutableValueStorage vs, boolean writeTag) throws AsterixException {
        vs.reset();
        try {
            DataOutput output = vs.getDataOutput();
            if (writeTag) {
                output.write(ATypeTag.STRING.serialize());
            }
            aString.setValue(str);
            strSerde.serialize(aString, output);
        } catch (IOException e) {
            throw new AsterixException("Could not serialize " + str);
        }
    }

    public void serializeString(String str, IMutableValueStorage vs) throws AsterixException {
        serializeString(str, vs, false);
    }
}
