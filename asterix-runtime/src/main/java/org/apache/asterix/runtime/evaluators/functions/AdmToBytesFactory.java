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

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AInt16;
import org.apache.asterix.om.base.AMutableInt16;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.typecomputer.impl.AnnotatedBytesTypeComputer;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.visitors.adm.PrintAdmBytesHelper;
import org.apache.asterix.runtime.evaluators.visitors.adm.PrintAdmBytesVisitor;
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
import java.util.List;

/**
 *
 * Check each field in a record and prints out the serialized values of the fields
 *
 */
public class AdmToBytesFactory implements ICopyEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private ICopyEvaluatorFactory inputEvalFactory;
    private ICopyEvaluatorFactory levelEvalFactory;
    private IAType outputType;
    private IAType inputArgType;

    private static final byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();

    public AdmToBytesFactory(ICopyEvaluatorFactory inputEvalFactory, ICopyEvaluatorFactory levelEvalFactory,
            IAType inputArgType, IAType outputType) {
        this.levelEvalFactory = levelEvalFactory;
        this.inputEvalFactory = inputEvalFactory;
        this.outputType = outputType;
        this.inputArgType = inputArgType;
    }


    @Override public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
        final ARecordType requiredRecType;
        try {
            requiredRecType = new ARecordType(outputType.getTypeName(), ((ARecordType)outputType).getFieldNames(),
                    ((ARecordType)outputType).getFieldTypes(),  ((ARecordType)outputType).isOpen());
        } catch (AsterixException | HyracksDataException e) {
            throw new IllegalStateException();
        }

        final ByteArrayAccessibleOutputStream outputStream = new ByteArrayAccessibleOutputStream();
        final DataInputStream dis = new DataInputStream(
                new ByteArrayInputStream(outputStream.getByteArray()));

        final AMutableInt16 byteIntValue = new AMutableInt16((short) 0);
        final ISerializerDeserializer<AInt16> intSerde =
                AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT16);

        return new ICopyEvaluator() {
            private DataOutput out = output.getDataOutput();

            private final PointableUtils pointableUtils = PointableUtils.INSTANCE;


            private PointableAllocator pa = new PointableAllocator();
            private final IVisitablePointable levelAccessor = pa.allocateEmpty();
            private final IVisitablePointable inputAccessor = PointableUtils.allocatePointable(pa, inputArgType);
            private final IVisitablePointable resultAccessor = pa.allocateEmpty();

            private final ArrayBackedValueStorage outInput0 = new ArrayBackedValueStorage();
            private final ICopyEvaluator eval0 = inputEvalFactory.createEvaluator(outInput0);
            private final ArrayBackedValueStorage buffer = new ArrayBackedValueStorage();
            private final ICopyEvaluator eval1 = levelEvalFactory.createEvaluator(buffer);


            private final PrintAdmBytesVisitor visitor = new PrintAdmBytesVisitor();

            @SuppressWarnings("unchecked")
            private final ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.ANULL);

            @Override public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                outInput0.reset();
                eval0.evaluate(tuple);
                buffer.reset();
                eval1.evaluate(tuple);

                if (outInput0.getByteArray()[0] == SER_NULL_TYPE_TAG) {
                    try {
                        nullSerde.serialize(ANull.NULL, out);
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                }
                // Get the serialized values from an input
                inputAccessor.set(outInput0);
                visitor.resetPrintHelper();

                try {
                    // Get the level
                    long level = getMaxLevel();
                    // Return only an array of raw byte
                    if (level == 0) {
                        printRawBytes(inputAccessor);
                    } else {
                       if (PointableUtils.isType(ATypeTag.RECORD, inputAccessor)) {
                            printAnnotated((ARecordVisitablePointable) inputAccessor, level);
                        } else {
                            visitor.setMaxLevel(level);
                            Triple<IVisitablePointable, IAType, Long> arg = new Triple<IVisitablePointable, IAType, Long>(
                                    resultAccessor, requiredRecType, 0L);
                            inputAccessor.accept(visitor, arg);
                            out.write(resultAccessor.getByteArray(), resultAccessor.getStartOffset(), resultAccessor.getLength());
                       }
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
                    String s = pointableUtils.deserializeString(levelAccessor);
                    if (s.equals("INF")) {
                        return Long.MAX_VALUE;
                    } else {
                        return Integer.parseInt(s);
                    }
                } else {
                    return pointableUtils.deserializeInt(levelAccessor);
                }
            }


            private void printRawBytes(IValueReference vr) throws IOException {
                OrderedListBuilder listBuilder = new OrderedListBuilder();
                AOrderedListType reqListType = new AOrderedListType((AOrderedListType) outputType.getType(),
                        outputType.getTypeName());
                listBuilder.reset(reqListType);
                byte[] bytes = vr.getByteArray();

                for(int i=vr.getStartOffset(); i<vr.getLength(); i++) {
                    byteIntValue.setValue(bytes[i]);
                    buffer.reset();
                    intSerde.serialize(byteIntValue, buffer.getDataOutput());
                    listBuilder.addItem(buffer);
                }

                listBuilder.write(out, true);
            }

            private void printAnnotated(ARecordVisitablePointable recordVisitablePointable, long maxLevel)
                    throws AlgebricksException {
                try {
                    PrintAdmBytesHelper ph = visitor.getPrintHelper();
                    RecordBuilder recordBuilder = new RecordBuilder();
                    recordBuilder.init();
                    recordBuilder.reset(requiredRecType);

                    List<IVisitablePointable> fnames = recordVisitablePointable.getFieldNames();
                    List<IVisitablePointable> fvalues = recordVisitablePointable.getFieldValues();

                    IVisitablePointable valuePointable = pa.allocateRecordValue(AnnotatedBytesTypeComputer.annotatedBytesType);
                    for (int i = 0; i < fnames.size(); i++) {
                        ph.printAnnotatedBytes(fvalues.get(i), valuePointable);
                        pointableUtils.addField(recordBuilder, fnames.get(i), valuePointable);
                    }
                    recordBuilder.write(out, true);

                } catch(IOException | AsterixException e) {
                    throw new AlgebricksException("Error in building output record");
                }

            }
        };
    }
}
