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
package org.apache.asterix.runtime.evaluators.functions.records;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.visitors.PrintSerializedRecordVisitor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * Check each field in a record and prints out the serialized values of the fields
 *
 */
public class RecordSerializationInfoFactory implements ICopyEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private ICopyEvaluatorFactory recordEvalFactory;
    private ICopyEvaluatorFactory integerEvalFactory;
    private ARecordType inputRecType;



    private final byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    private static final byte SER_INT_TYPE_TAG = ATypeTag.INT64.serialize();
    private static final byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();

    public RecordSerializationInfoFactory (ICopyEvaluatorFactory recordEvalFactory, ICopyEvaluatorFactory integerEvalFactory, ARecordType inputRecType) {
        this.recordEvalFactory = recordEvalFactory;
        this.inputRecType = inputRecType;
        this.integerEvalFactory = integerEvalFactory;
    }


    @Override public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
        final PointableAllocator pa = new PointableAllocator();
        final IVisitablePointable vp0 = pa.allocateRecordValue(inputRecType);

        final ByteArrayAccessibleOutputStream outputStream = new ByteArrayAccessibleOutputStream();
        final DataInputStream dis = new DataInputStream(
                new ByteArrayInputStream(outputStream.getByteArray()));

        return new ICopyEvaluator() {
            private DataOutput out = output.getDataOutput();
            private ByteArrayAccessibleOutputStream subRecordTmpStream = new ByteArrayAccessibleOutputStream();

            private ArrayBackedValueStorage outInput0 = new ArrayBackedValueStorage();
            private ICopyEvaluator eval0 = recordEvalFactory.createEvaluator(outInput0);
            private ArrayBackedValueStorage outInput1 = new ArrayBackedValueStorage();
            private ICopyEvaluator eval1 = integerEvalFactory.createEvaluator(outInput1);

            private final PrintSerializedRecordVisitor visitor = new PrintSerializedRecordVisitor();

            @SuppressWarnings("unchecked")
            private final ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.ANULL);

            @Override public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                outInput0.reset();
                eval0.evaluate(tuple);
                outInput1.reset();
                eval1.evaluate(tuple);

                if (outInput0.getByteArray()[0] == SER_NULL_TYPE_TAG) {
                    try {
                        nullSerde.serialize(ANull.NULL, out);
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                }

                // Input arg type check
                byte inputBytes1[] = outInput1.getByteArray();
                if (inputBytes1[0] != SER_INT_TYPE_TAG && inputBytes1[0] != SER_STRING_TYPE_TAG) {
                    throw new AlgebricksException(AsterixBuiltinFunctions.ADM_TO_BYTES.getName()
                            + ": expects input type (RECORD, INT32|STRING) but got ("
                            + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outInput0.getByteArray()[0])
                            + ", "
                            + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outInput1.getByteArray()[0])
                            + ")");
                }

                // Get the serialized values from an input
                vp0.set(outInput0);

                ATypeTag typeTag1 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(inputBytes1[0]);
                try {
                    //out.write(ATypeTag.STRING.serialize());
                    Triple<IVisitablePointable, StringBuilder, Long> arg =
                            new Triple<IVisitablePointable, StringBuilder, Long>(null, new StringBuilder(), 0L);
                    visitor.setMaxLevel(getLevel(outInput1, typeTag1));
                    vp0.accept(visitor, arg);
                    AString aString = new AString(arg.second.toString());
                    out.writeByte(ATypeTag.STRING.serialize());
                    AStringSerializerDeserializer.INSTANCE.serialize(aString, out);
                } catch (HyracksDataException e) {
                    throw new AlgebricksException("Unable to display serilized value of " +
                            ((ARecordVisitablePointable) vp0).getInputRecordType());
                } catch (AsterixException e) {
                    throw new AlgebricksException("Unable to display serilized value of " +
                            ((ARecordVisitablePointable) vp0).getInputRecordType());
                } catch (IOException e) {
                    new AlgebricksException("Error parsing input argument #2.");
                }

            }

            private long getLevel(IValueReference valueReference, ATypeTag typeTag) throws IOException {
                long level = 0;
                outputStream.reset();
                outputStream.write(valueReference.getByteArray(),
                        valueReference.getStartOffset() + 1, valueReference.getLength());
                dis.reset();

                if (typeTag == ATypeTag.STRING) {
                    String s = AStringSerializerDeserializer.INSTANCE.deserialize(dis).getStringValue();
                    if (s.equals("INF")) {
                        level = Long.MAX_VALUE;
                    } else {
                        level = Integer.parseInt(s);
                    }
                } else {
                    level = AInt64SerializerDeserializer.INSTANCE.deserialize(dis).getLongValue();
                }

                return level;
            }

        };
    }
}
