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
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.visitors.admdebugging.PrintAdmBytesVisitor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * Check each field in a record and prints out the serialized values of the fields
 *
 */
public class AdmToBytesFactory implements ICopyEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private ICopyEvaluatorFactory inputEvalFactory;
    private ICopyEvaluatorFactory levelEvalFactory;
    private IAType outputRecType;
    private IAType inputArgType;

    private static final byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();

    public AdmToBytesFactory(ICopyEvaluatorFactory inputEvalFactory, ICopyEvaluatorFactory levelEvalFactory,
            IAType inputArgType, IAType outputRecType) {
        this.levelEvalFactory = levelEvalFactory;
        this.inputEvalFactory = inputEvalFactory;
        this.outputRecType = outputRecType;
        this.inputArgType = inputArgType;
    }


    @Override public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
        final ARecordType requiredRecType;
        try {
            // Clone to avoid racing conditions
            requiredRecType = new ARecordType(outputRecType.getTypeName(), ((ARecordType)outputRecType).getFieldNames(),
                    ((ARecordType)outputRecType).getFieldTypes(),  ((ARecordType)outputRecType).isOpen());
        } catch (AsterixException | HyracksDataException e) {
            throw new IllegalStateException();
        }

        final PointableUtils pu = new PointableUtils();
        final ArrayBackedValueStorage  rawBytesFieldNameBuffer = pu.getTempBuffer();
        final String RAW_BYTES_FIELD_NAME = "RawBytes";
        rawBytesFieldNameBuffer.reset();
        try {
            AStringSerializerDeserializer.INSTANCE.serialize(new AString(RAW_BYTES_FIELD_NAME),
                    rawBytesFieldNameBuffer.getDataOutput());
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }

        return new ICopyEvaluator() {
            private DataOutput out = output.getDataOutput();
            private PointableValueDecoder pvd = new PointableValueDecoder(pu);


            private PointableAllocator pa = new PointableAllocator();
            private final IVisitablePointable levelAccessor = pa.allocateEmpty();
            private final IVisitablePointable inputAccessor = PointableUtils.allocatePointable(pa, inputArgType);
            private final IVisitablePointable resultAccessor = pa.allocateRecordValue(requiredRecType);

            private final ArrayBackedValueStorage outInput0 = new ArrayBackedValueStorage();
            private final ICopyEvaluator eval0 = inputEvalFactory.createEvaluator(outInput0);
            private final ArrayBackedValueStorage buffer = new ArrayBackedValueStorage();
            private final ICopyEvaluator eval1 = levelEvalFactory.createEvaluator(buffer);


            private PrintAdmBytesVisitor visitor;

            @SuppressWarnings("unchecked")
            private final ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.ANULL);

            @Override public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                outInput0.reset();
                eval0.evaluate(tuple);
                buffer.reset();
                eval1.evaluate(tuple);

                pu.resetObjectPools();

                if (outInput0.getByteArray()[0] == SER_NULL_TYPE_TAG) {
                    try {
                        nullSerde.serialize(ANull.NULL, out);
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                }
                // Get the serialized values from an input
                inputAccessor.set(outInput0);

                try {
                    // Get the level
                    long level = getMaxLevel();
                    visitor = new PrintAdmBytesVisitor(pvd, pu, level);
                    // Return only an array of raw byte
                    if (level < 1) {
                        printRawBytes(inputAccessor);
                    } else {
                            Triple<IVisitablePointable, IAType, Long> arg =
                                    new Triple<IVisitablePointable, IAType, Long>(resultAccessor, requiredRecType, 1L);
                            inputAccessor.accept(visitor, arg);
                            out.write(resultAccessor.getByteArray(), resultAccessor.getStartOffset(),
                                    resultAccessor.getLength());
                    }
                } catch (AsterixException | HyracksDataException e) {
                    throw new AlgebricksException("Unable to display serilized value of " +
                            ((ARecordVisitablePointable) inputAccessor).getInputRecordType());
                } catch (IOException e) {
                    new AlgebricksException("Error parsing input argument #2.");
                }

            }

            private long getMaxLevel() throws IOException, AlgebricksException {
                if(buffer.getByteArray()[0] == SER_NULL_TYPE_TAG)
                    return 0;

                // Level input arg type check
                levelAccessor.set(buffer);
                ATypeTag tag = PointableUtils.getTypeTag(levelAccessor);
                if (tag != ATypeTag.INT64 && tag != ATypeTag.STRING) {
                    throw new AlgebricksException(AsterixBuiltinFunctions.ADM_TO_BYTES.getName()
                            + ": expects input type (ADM object, INT32|STRING) but got ("
                            + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outInput0.getByteArray()[0])
                            + ", " + tag + ")");
                }

                if (tag == ATypeTag.STRING) {
                    String s = pu.deserializeString(levelAccessor);
                    if (s.equals("INF")) {
                        return Long.MAX_VALUE;
                    } else {
                        return Integer.parseInt(s);
                    }
                } else {
                    return pu.deserializeInt(levelAccessor);
                }
            }

            // Print a raw byte array
            private void printRawBytes(IValueReference vr)
                    throws AlgebricksException {
                try {
                    byte[] b = vr.getByteArray();
                    int offset = vr.getStartOffset();
                    int length = vr.getLength();

                    IARecordBuilder recordBuilder = pu.getRecordBuilder();
                    recordBuilder.reset(requiredRecType);
                    recordBuilder.init();

                    IVisitablePointable byteArrayBuffer = pa.allocateFieldValue(BuiltinType.ASTRING);
                    pvd.setByteArrayPointableValue(b, offset, length, byteArrayBuffer);

                    int pos = -1;
                    if ((pos = requiredRecType.findFieldPosition(RAW_BYTES_FIELD_NAME))>=0) {
                        recordBuilder.addField(pos, byteArrayBuffer);
                    } else {
                        recordBuilder.addField(rawBytesFieldNameBuffer, byteArrayBuffer);
                    }
                    recordBuilder.write(out, true);
                } catch (AsterixException | IOException e) {
                    throw new AlgebricksException(e);
                }
            }
        };
    }
}
