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
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AInt16;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
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
import java.util.ArrayList;

/**
 *
 * Check each field in a record and prints out the serialized values of the fields
 *
 */
public class AdmToBytesFactory implements ICopyEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private ICopyEvaluatorFactory recordEvalFactory;
    private ICopyEvaluatorFactory integerEvalFactory;
    private IAType outputType;
    private IAType inputArgType;

    private static final byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    private static final byte SER_INT_TYPE_TAG = ATypeTag.INT64.serialize();
    private static final byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();

    private final static ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ASTRING);

    private final static AString tagName = new AString("tag");
    private final static AString lengthName = new AString("length");
    private final static AString valueName = new AString("value");

    private PrintAdmBytesHelper printHelper;


    public AdmToBytesFactory(ICopyEvaluatorFactory recordEvalFactory, ICopyEvaluatorFactory integerEvalFactory,
            IAType inputArgType, IAType outputType) {
        this.recordEvalFactory = recordEvalFactory;
        this.outputType = outputType;
        this.inputArgType = inputArgType;
        this.integerEvalFactory = integerEvalFactory;
    }


    @Override public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
        final PointableAllocator pa = new PointableAllocator();
        final IVisitablePointable vp0;
        switch (inputArgType.getTypeTag()) {
            case RECORD:
                vp0 = pa.allocateRecordValue(inputArgType);
                break;
            case ORDEREDLIST:
            case UNORDEREDLIST:
                vp0 = pa.allocateListValue(inputArgType);
                break;
            default:
                vp0 = pa.allocateFieldValue(inputArgType);
        }


        final ByteArrayAccessibleOutputStream outputStream = new ByteArrayAccessibleOutputStream();
        final DataInputStream dis = new DataInputStream(
                new ByteArrayInputStream(outputStream.getByteArray()));

        return new ICopyEvaluator() {
            private DataOutput out = output.getDataOutput();

            private ArrayBackedValueStorage outInput0 = new ArrayBackedValueStorage();
            private ICopyEvaluator eval0 = recordEvalFactory.createEvaluator(outInput0);
            private ArrayBackedValueStorage outInput1 = new ArrayBackedValueStorage();
            private ICopyEvaluator eval1 = integerEvalFactory.createEvaluator(outInput1);

            private final PrintAdmBytesVisitor visitor = new PrintAdmBytesVisitor();

            private Object builder = (outputType.getTypeTag()==ATypeTag.ORDEREDLIST)? new OrderedListBuilder():
                    new RecordBuilder();

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
                            + ": expects input type (ADM object, INT32|STRING) but got ("
                            + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outInput0.getByteArray()[0])
                            + ", "
                            + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outInput1.getByteArray()[0])
                            + ")");
                }

                // Get the serialized values from an input
                vp0.set(outInput0);

                visitor.resetPrintHelper();
                printHelper = visitor.getPrintHelper();

                ATypeTag typeTag1 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(inputBytes1[0]);
                try {
                    // Get the level
                    long level = getLevel(outInput1, typeTag1);

                    // Return only an array of raw byte
                    if (level == 0) {
                        printRawBytes(vp0);
                    } else if (level == 1){
                        // only one level
                        /*PrintAdmBytesHelper printHelper = visitor.getPrintHelper();
                        printHelper.printAnnotatedBytes(vp0, out);*/
                        printAnnotatedBytes(vp0);
                    } else {
                        Triple<ATypeTag, Object, Long> arg;
                        switch (inputArgType.getTypeTag()) {
                            case RECORD:
                                RecordBuilder recBuilder = (RecordBuilder) builder;
                                recBuilder.init();
                                recBuilder.reset((ARecordType) outputType);
                                arg = new Triple<ATypeTag, Object, Long>(inputArgType.getTypeTag(), recBuilder, 1L);
                                visitor.setMaxLevel(level);
                                ((ARecordVisitablePointable)vp0).accept(visitor, arg);
                                ((RecordBuilder)arg.second).write(out, true);
                                break;
                            case ORDEREDLIST:
                            case UNORDEREDLIST:
                                OrderedListBuilder listBuilder = (OrderedListBuilder) builder;
                                listBuilder.reset((AOrderedListType)outputType);
                                arg = new Triple<ATypeTag, Object, Long>(inputArgType.getTypeTag(), listBuilder, 1L);
                                visitor.setMaxLevel(level);
                                ((AListVisitablePointable)vp0).accept(visitor, arg);
                                ((OrderedListBuilder)arg.second).write(out, true);
                                break;
                            default: // only one level
                                PrintAdmBytesHelper printHelper = visitor.getPrintHelper();
                                printHelper.printAnnotatedBytes(vp0, out);
                        }
                    }
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
                outputStream.write(valueReference.getByteArray(), valueReference.getStartOffset() + 1,
                        valueReference.getLength());
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


            private void printRawBytes(IValueReference vr) throws IOException {
                ArrayList<IAObject> byteList = new ArrayList<>();
                byte[] bytes = vr.getByteArray();
                for(int i=vr.getStartOffset(), j=0; i<vr.getLength(); i++, j++) {
                    byteList.add(new AInt16((short)(bytes[i])));
                }

                AOrderedList byteAList = new AOrderedList((AOrderedListType) outputType, byteList);
                ISerializerDeserializer listSerde = AqlSerializerDeserializerProvider.INSTANCE.
                        getNonTaggedSerializerDeserializer((AOrderedListType) outputType);

                out.write(ATypeTag.ORDEREDLIST.serialize());
                listSerde.serialize(new AOrderedList((AOrderedListType) outputType, byteList), out);
            }

            private void printAnnotatedBytes(IValueReference vr) throws IOException, AsterixException {
                byte[] bytes = vr.getByteArray();
                int offset = vr.getStartOffset();
                //int length = vr.getLength(); // total byte array length
                int valueOffset = offset + PrintAdmBytesHelper.getValueOffset(bytes[offset]);
                int valueLength = vr.getLength() - valueOffset + offset; // value length
                int lengthBytesLength = valueOffset - offset - 1; // value length

                AString tagByteValue = new AString(printHelper.byteArrayToString(bytes, 0, 1, false));
                AString lengthBytesValue = new AString(printHelper.byteArrayToString(bytes, offset+1,
                        lengthBytesLength, false));
                AString valueBytesValue = new AString(printHelper.byteArrayToString(bytes, valueOffset,
                        valueLength, false));

                RecordBuilder recordBuilder = new RecordBuilder();
                recordBuilder.init();
                recordBuilder.reset((ARecordType) outputType);
                addField(tagName, tagByteValue, recordBuilder);
                addField(lengthName, lengthBytesValue, recordBuilder);
                addField(valueName, valueBytesValue, recordBuilder);

                recordBuilder.write(out, true);


            }

            private void addField(AString fname, AString fvalue, RecordBuilder recordBuilder)
                    throws HyracksDataException, AsterixException {
                ArrayBackedValueStorage fieldAbvs = new ArrayBackedValueStorage();
                ArrayBackedValueStorage valueAbvs = new ArrayBackedValueStorage();
                IVisitablePointable fieldPointableValue = pa.allocateFieldValue(BuiltinType.ASTRING);
                IVisitablePointable valuePointableValue = pa.allocateFieldValue(BuiltinType.ASTRING);

                fieldAbvs.reset();
                valueAbvs.reset();
                stringSerde.serialize(fname, fieldAbvs.getDataOutput());
                stringSerde.serialize(fvalue, valueAbvs.getDataOutput());

                fieldPointableValue.set(fieldAbvs);
                valuePointableValue.set(valueAbvs);

                recordBuilder.addField(fieldPointableValue, valuePointableValue);

            }

        };
    }
}
