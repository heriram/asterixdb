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
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.typecomputer.impl.TypeComputerUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.admdebugger.FieldTypeComputerUtils;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
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
import java.util.ArrayList;
import java.util.List;


public class AnnotatedBytesDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    private static final byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new AnnotatedBytesDescriptor();
        }
    };

    private ARecordType outRecType;
    private IAType inputType;
    private IAType inputLevelType;

    public void reset(IAType outType, IAType inType, IAType inputLevelType) {
        outRecType = TypeComputerUtils.extractRecordType(outType);

        switch (inType.getTypeTag()) {
            case RECORD:
                this.inputType = TypeComputerUtils.extractRecordType(inType);
                break;
            case UNORDEREDLIST:
                this.inputType = TypeComputerUtils.extractUnorderedListType(inType);
                break;
            case ORDEREDLIST:
                this.inputType = TypeComputerUtils.extractOrderedListType(inType);
                break;
            default:
                this.inputType = inType;
        }

        this.inputLevelType = inputLevelType;
    }

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @SuppressWarnings("unchecked")
            private final ISerializerDeserializer<ANull> nullSerDe = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.ANULL);

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                final DataOutput out = output.getDataOutput();

                final PointableAllocator pa = new PointableAllocator();
                final IVisitablePointable vp0 = PointableUtils.allocatePointable(pa, inputType);
                final IVisitablePointable vp1 = pa.allocateFieldValue(inputLevelType);

                final ArrayBackedValueStorage abvs0 = new ArrayBackedValueStorage();
                final ArrayBackedValueStorage abvs1 = new ArrayBackedValueStorage();

                final ICopyEvaluator eval0 = args[0].createEvaluator(abvs0);
                final ICopyEvaluator eval1 = args[1].createEvaluator(abvs1);

                final List<IARecordBuilder> rbStack = new ArrayList<>();

                final PointableUtils pu = new PointableUtils();

                final AString rawBytesFieldName = new AString("RawBytes");

                return new ICopyEvaluator() {

                    private PointableValueDecoder pvd = new PointableValueDecoder(pu);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        ARecordType recType;
                        try {
                            recType = new ARecordType(outRecType.getTypeName(), outRecType.getFieldNames(),
                                    outRecType.getFieldTypes(), outRecType.isOpen());
                        } catch (AsterixException | HyracksDataException e) {
                            throw new IllegalStateException();
                        }

                        pu.resetObjectPools();

                        abvs0.reset();
                        abvs1.reset();

                        eval0.evaluate(tuple);
                        eval1.evaluate(tuple);

                        if (abvs0.getByteArray()[0] == SER_NULL_TYPE_TAG) {
                            try {
                                nullSerDe.serialize(ANull.NULL, out);
                            } catch (HyracksDataException e) {
                                throw new AlgebricksException(e);
                            }
                            return;
                        }
                        vp0.set(abvs0);

                        long maxLevel = 0;
                        if (abvs1.getByteArray()[0] != SER_NULL_TYPE_TAG) {
                            vp1.set(abvs1);
                            maxLevel = getMaxLevel((AFlatValuePointable) vp1);
                        }

                        try {

                            if (maxLevel==0) {
                                printRawBytes(vp0);
                            } else if (maxLevel==1 || !PointableUtils.isType(ATypeTag.RECORD, vp0)) {
                                ARecordType annoRecType = FieldTypeComputerUtils.getAnnotatedBytesRecordType(
                                        EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(vp0.getByteArray()[0]));
                                ARecordVisitablePointable resultAccessor = pvd.getAnnotatedByteArray(vp0, annoRecType);
                                out.write(resultAccessor.getByteArray(), resultAccessor.getStartOffset(),
                                        resultAccessor.getLength());
                            } else {
                                printRecordFields(recType, (ARecordVisitablePointable) vp0, 0, maxLevel);
                                rbStack.get(0).write(out, true);
                            }

                        } catch (IOException | AsterixException e) {
                            throw new AlgebricksException(e);
                        }
                    }


                    private void printRawBytes(IValueReference vr)
                            throws AlgebricksException {
                        try {
                            byte[] b = vr.getByteArray();
                            int offset = vr.getStartOffset();
                            int length = vr.getLength();

                            IARecordBuilder recordBuilder = pu.getRecordBuilder();
                            recordBuilder.reset(outRecType);
                            recordBuilder.init();

                            IVisitablePointable byteArrayBuffer = pa.allocateFieldValue(BuiltinType.ASTRING);
                            pvd.setByteArrayPointableValue(b, offset, length, byteArrayBuffer);

                            int pos = -1;
                            if ((pos = outRecType.findFieldPosition(rawBytesFieldName.getStringValue()))>=0) {
                                recordBuilder.addField(pos, byteArrayBuffer);
                            } else {
                                ISerializerDeserializer strSerde = AqlSerializerDeserializerProvider.INSTANCE.
                                        getSerializerDeserializer(BuiltinType.ASTRING);
                                ArrayBackedValueStorage buffer = pu.getTempBuffer();
                                buffer.reset();
                                strSerde.serialize(rawBytesFieldName, buffer.getDataOutput());
                                recordBuilder.addField(buffer, byteArrayBuffer);
                            }


                            recordBuilder.write(out, true);
                        } catch (AsterixException | IOException e) {
                            throw new AlgebricksException(e);
                        }
                    }


                    private void printRecordFields(ARecordType requiredType, ARecordVisitablePointable rp,
                            int nestedLevel, long maxLevel) throws IOException,
                            AsterixException, AlgebricksException {

                        if (rbStack.size() < (nestedLevel + 1)) {
                            rbStack.add(pu.getRecordBuilder());
                        }

                        rbStack.get(nestedLevel).reset(requiredType);
                        rbStack.get(nestedLevel).init();

                        List<IVisitablePointable> fnames = rp.getFieldNames();
                        List<IVisitablePointable> fvalues = rp.getFieldValues();
                        List<IVisitablePointable> ftypes = rp.getFieldTypeTags();
                        for (int i = 0; i < rp.getFieldNames().size(); i++) {
                            // maxLevel=2 print the annotated bytes of the fields
                            if (maxLevel==2) {
                                addAnnotatedField(requiredType, fnames.get(i), fvalues.get(i), nestedLevel);
                            } else {
                                addNestedField(requiredType, fnames.get(i), fvalues.get(i), ftypes.get(i), nestedLevel, maxLevel);
                            }
                        }
                    }

                    private long getMaxLevel(AFlatValuePointable lp) throws AlgebricksException {
                        ATypeTag tag = PointableUtils.getTypeTag(lp);
                        if (tag != ATypeTag.INT64 && tag != ATypeTag.STRING) {
                            throw new AlgebricksException(AsterixBuiltinFunctions.ADM_TO_BYTES.getName()
                                    + ": expects input type (ADM object, INT32|STRING) but got ("
                                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(vp0.getByteArray()[0])
                                    + ", " + tag + ")");
                        }

                        try {
                            if (tag == ATypeTag.STRING) {
                                String s = pu.deserializeString(lp);
                                if (s.equals("INF")) {
                                    return Long.MAX_VALUE;
                                } else {
                                    return Integer.parseInt(s);
                                }
                            } else {
                                return pu.deserializeInt(lp);
                            }
                        } catch (IOException e) {
                            throw new AlgebricksException("Exception ecountered when trying to deserialize the "
                                    + "level input");
                        }
                    }


                    private void addAnnotatedField(ARecordType requiredType, IVisitablePointable fieldNamePointable,
                            IVisitablePointable fieldValue, int nestedLevel) throws AlgebricksException {

                        String fieldName = null;
                        IVisitablePointable annotatedBytes;
                        try {
                            fieldName = pu.getFieldName(fieldNamePointable);

                            if (requiredType != null && requiredType.isClosedField(fieldName)) {
                                int pos = requiredType.findFieldPosition(fieldName);
                                IAType recType = requiredType.getFieldType(fieldName);
                                if (recType != null && recType.getTypeTag() == ATypeTag.RECORD) {
                                    annotatedBytes = pvd.getAnnotatedByteArray(fieldValue, (ARecordType) recType);
                                } else {
                                    annotatedBytes = pvd.getAnnotatedByteArray(fieldValue);
                                }
                                rbStack.get(nestedLevel).addField(pos, annotatedBytes);
                            } else {
                                annotatedBytes = pvd.getAnnotatedByteArray(fieldValue);
                                rbStack.get(nestedLevel).addField(fieldNamePointable, annotatedBytes);
                            }
                        } catch (IOException | AsterixException e) {
                            throw new AlgebricksException("Error adding field values for " + fieldName);
                        }
                    }

                    private void addNestedField(ARecordType requiredType, IVisitablePointable fieldNamePointable,
                            IVisitablePointable fieldValue, IVisitablePointable fieldType, int nestedLevel,
                            long maxLevel) throws IOException, AsterixException, AlgebricksException {

                        if (nestedLevel == (maxLevel-2) || !PointableUtils.isType(ATypeTag.RECORD, fieldType)) {
                            addAnnotatedField(requiredType, fieldNamePointable, fieldValue, nestedLevel);
                        } else {
                            String fieldName = pu.getFieldName(fieldNamePointable);
                            ArrayBackedValueStorage tabvs = pu.getTempBuffer();
                            tabvs.reset();
                            int pos = -1;
                            if (requiredType != null && (pos = requiredType.findFieldPosition(fieldName)) > -1) {
                                printRecordFields((ARecordType) requiredType.getFieldType(fieldName),
                                        (ARecordVisitablePointable) fieldValue, nestedLevel + 1, maxLevel);
                                rbStack.get(nestedLevel + 1).write(tabvs.getDataOutput(), true);
                                rbStack.get(nestedLevel).addField(pos, tabvs);
                            } else {
                                ARecordType ct = null;
                                if (requiredType != null) {
                                    ct = (ARecordType) requiredType.getFieldType(fieldName);
                                }
                                printRecordFields(ct, (ARecordVisitablePointable) fieldValue, nestedLevel + 1, maxLevel);
                                rbStack.get(nestedLevel + 1).write(tabvs.getDataOutput(), true);
                                rbStack.get(nestedLevel).addField(fieldNamePointable, tabvs);
                            }
                        }
                    }

                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.ADM_TO_BYTEARRAY;
    }
}
