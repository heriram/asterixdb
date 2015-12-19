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
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.runtime.RuntimeRecordTypeInfo;
import org.apache.asterix.om.util.ResettableByteArrayOutputStream;
import org.apache.asterix.runtime.evaluators.visitors.admdebugging.AdmToBytesVisitor;
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
import org.apache.hyracks.util.string.UTF8StringUtil;

/**
 * Check each field in a record and prints out the serialized values of the fields
 */
public class AdmToBytesFactory implements ICopyEvaluatorFactory {
    private static final byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    private ICopyEvaluatorFactory inputEvalFactory;
    private ICopyEvaluatorFactory levelEvalFactory;
    private IAType outputType;
    private IAType inputArgType;

    public AdmToBytesFactory(ICopyEvaluatorFactory inputEvalFactory, ICopyEvaluatorFactory levelEvalFactory,
            IAType inputArgType, IAType outputType) {
        this.levelEvalFactory = levelEvalFactory;
        this.inputEvalFactory = inputEvalFactory;
        this.outputType = outputType;
        this.inputArgType = inputArgType;
    }

    @Override
    public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
        @SuppressWarnings("unchecked")
        final ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.ANULL);

        return new ICopyEvaluator() {
            private final AString RAW_BYTES_FIELD_NAME = new AString("RawBytes");
            private final AString INFINITY_STR = new AString("INF");
            private final RuntimeRecordTypeInfo runtimeRecordTypeInfo = new RuntimeRecordTypeInfo();
            private final AStringSerializerDeserializer aStringSerDer = AStringSerializerDeserializer.INSTANCE;
            private final ArrayBackedValueStorage outInput0 = new ArrayBackedValueStorage();
            private final ICopyEvaluator eval0 = inputEvalFactory.createEvaluator(outInput0);
            private final ArrayBackedValueStorage outInput1 = new ArrayBackedValueStorage();
            private final ICopyEvaluator eval1 = levelEvalFactory.createEvaluator(outInput1);
            private final ResettableByteArrayOutputStream bos = new ResettableByteArrayOutputStream();
            private final DataOutputStream dos = new DataOutputStream(bos);
            private final IARecordBuilder recordBuilder = new RecordBuilder();
            private final AdmToBytesHelper admToBytesHelper = new AdmToBytesHelper(new PointableHelper());
            private final PointableAllocator allocator = new PointableAllocator();
            private final IVisitablePointable levelPointable = allocator.allocateEmpty();
            private final IVisitablePointable inputPointable = PointableHelper
                    .allocatePointable(allocator, inputArgType);
            private final IVisitablePointable tempReference = allocator.allocateEmpty();
            private AdmToBytesVisitor visitor;

            @Override
            public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                outInput0.reset();
                eval0.evaluate(tuple);
                outInput1.reset();
                eval1.evaluate(tuple);
                bos.reset();

                if (outInput0.getByteArray()[0] == SER_NULL_TYPE_TAG) {
                    try {
                        nullSerde.serialize(ANull.NULL, output.getDataOutput());
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                }
                // Get the serialized values from an input
                inputPointable.set(outInput0);
                if (outputType.getTypeTag() == ATypeTag.RECORD) {
                    runtimeRecordTypeInfo.reset((ARecordType) outputType);
                }
                try {
                    // Get the level
                    long outputLevel = getOutputLevel();
                    visitor = new AdmToBytesVisitor(admToBytesHelper, outputLevel);
                    // Return only an array of raw byte
                    if (outputLevel < 1) {
                        printRawBytes(inputPointable);
                    } else {
                        Triple<IAType, RuntimeRecordTypeInfo, Long> arg = new Triple<>(outputType,
                                runtimeRecordTypeInfo, 1L);
                        IVisitablePointable resultPointable = inputPointable.accept(visitor, arg);
                        output.getDataOutput().write(resultPointable.getByteArray(), resultPointable.getStartOffset(),
                                resultPointable.getLength());
                    }
                } catch (AsterixException | HyracksDataException e) {
                    throw new AlgebricksException("Unable to display serialized value of the input " + inputArgType);
                } catch (IOException e) {
                    new AlgebricksException("Error parsing input argument #2.");
                }

            }

            private long getOutputLevel() throws AlgebricksException {
                if (outInput1.getByteArray()[0] == SER_NULL_TYPE_TAG) {
                    return 0;
                }
                // Level input arg type check
                levelPointable.set(outInput1);
                ATypeTag tag = PointableHelper.getTypeTag(levelPointable);
                if (tag != ATypeTag.INT64 && tag != ATypeTag.STRING) {
                    throw new AlgebricksException(AsterixBuiltinFunctions.ADM_TO_BYTES.getName()
                            + ": expects input type (ADM object, INT32|STRING) but got ("
                            + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outInput0.getByteArray()[0]) + ", "
                            + tag + ")");
                }
                try {
                    if (tag == ATypeTag.STRING) {
                        // Serialize "INF"
                        int start = bos.size();
                        dos.writeByte(ATypeTag.STRING.serialize());
                        aStringSerDer.serialize(INFINITY_STR, dos);
                        int end = bos.size();
                        tempReference.set(bos.getByteArray(), start, end - start);
                        if (PointableHelper.isEqual(levelPointable, tempReference)) {
                            return Long.MAX_VALUE;
                        } else {
                            StringBuilder sb = new StringBuilder();
                            sb = UTF8StringUtil.toString(sb, levelPointable.getByteArray(),
                                    levelPointable.getStartOffset() + 1);
                            // Will throw an exception if not a number
                            return Long.parseLong(sb.toString());
                        }
                    } else {
                        return PointableHelper.getLongValue(levelPointable, true);
                    }
                } catch (IOException e) {
                    throw new AlgebricksException(e);
                }
            }

            // Print a raw byte array
            private void printRawBytes(IValueReference vr) throws AlgebricksException {
                try {
                    byte[] b = vr.getByteArray();
                    int offset = vr.getStartOffset();
                    int length = vr.getLength();

                    ARecordType reqRecType;
                    if (outputType != null && outputType.getTypeTag() == ATypeTag.RECORD) {
                        reqRecType = (ARecordType) outputType;
                    } else {
                        reqRecType = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
                    }
                    recordBuilder.reset(reqRecType);
                    recordBuilder.init();

                    int start = bos.size();
                    dos.writeByte(ATypeTag.STRING.serialize());
                    aStringSerDer.serialize(RAW_BYTES_FIELD_NAME, dos);
                    int end = bos.size();
                    tempReference.set(bos.getByteArray(), start, end - start);

                    int pos = runtimeRecordTypeInfo.getFieldIndex(tempReference.getByteArray(),
                            tempReference.getStartOffset() + 1, tempReference.getLength() - 1);
                    IVisitablePointable byteArrayBuffer = allocator.allocateFieldValue(BuiltinType.ASTRING);
                    if (pos >= 0) {
                        admToBytesHelper.setByteArrayStringPointableValue(b, offset, length, byteArrayBuffer, true);
                        recordBuilder.addField(pos, byteArrayBuffer);
                    } else {
                        admToBytesHelper.setByteArrayStringPointableValue(b, offset, length, byteArrayBuffer, true);
                        recordBuilder.addField(tempReference, byteArrayBuffer);
                    }
                    recordBuilder.write(output.getDataOutput(), true);
                } catch (AsterixException | IOException e) {
                    throw new AlgebricksException("Error parsing input argument #2. " + e);
                }
            }
        };
    }
}
