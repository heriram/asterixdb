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

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.typecomputer.impl.TypeComputerUtils;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PointableUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

import java.io.IOException;
import java.util.List;

public class RecordAddFieldsDescriptor  extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new RecordAddFieldsDescriptor();
        }
    };

    private ARecordType outRecType;
    private ARecordType inRecType;
    private AOrderedListType inListType;

    public void reset(IAType outType, IAType inType0, IAType inType1) {
        outRecType = TypeComputerUtils.extractRecordType(outType);
        inRecType = TypeComputerUtils.extractRecordType(inType0);
        inListType = TypeComputerUtils.extractOrderedListType(inType1);
    }


    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {

            private static final long serialVersionUID = 1L;
            private final byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();

            @SuppressWarnings("unchecked")
            private final ISerializerDeserializer<ANull> nullSerDe = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.ANULL);

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                final ARecordType recType;
                try {
                    recType = new ARecordType(outRecType.getTypeName(), outRecType.getFieldNames(),
                            outRecType.getFieldTypes(), outRecType.isOpen());
                } catch (AsterixException | HyracksDataException e) {
                    throw new IllegalStateException();
                }

                final PointableAllocator pa = new PointableAllocator();
                final IVisitablePointable vp0 = pa.allocateRecordValue(inRecType);
                final IVisitablePointable vp1 = pa.allocateListValue(inListType);


                final ArrayBackedValueStorage abvs0 = new ArrayBackedValueStorage();
                final ArrayBackedValueStorage abvs1 = new ArrayBackedValueStorage();

                final ICopyEvaluator eval0 = args[0].createEvaluator(abvs0);
                final ICopyEvaluator eval1 = args[1].createEvaluator(abvs1);

                final PointableUtils pu = new PointableUtils();

                final ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
                        .getSerializerDeserializer(BuiltinType.ASTRING);

                final ArrayBackedValueStorage fieldNamePointer = new ArrayBackedValueStorage();
                final ArrayBackedValueStorage fieldValuePointer = new ArrayBackedValueStorage();
                try {
                    fieldNamePointer.reset();
                    fieldValuePointer.reset();
                    stringSerde.serialize(new AString("field-name"), fieldNamePointer.getDataOutput());
                    stringSerde.serialize(new AString("field-value"), fieldValuePointer.getDataOutput());

                } catch (HyracksDataException e) {
                    e.printStackTrace();
                }

                final RecordBuilder recordBuilder = new RecordBuilder();

                return new ICopyEvaluator() {


                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        recordBuilder.reset(recType);
                        abvs0.reset();
                        abvs1.reset();

                        eval0.evaluate(tuple);
                        eval1.evaluate(tuple);

                        if (abvs0.getByteArray()[0] == SER_NULL_TYPE_TAG ||
                                abvs1.getByteArray()[0] == SER_NULL_TYPE_TAG) {
                            try {
                                nullSerDe.serialize(ANull.NULL, output.getDataOutput());
                            } catch (HyracksDataException e) {
                                throw new AlgebricksException(e);
                            }
                            return;
                        }

                        vp0.set(abvs0);
                        vp1.set(abvs1);

                        ARecordVisitablePointable recordPointable = (ARecordVisitablePointable) vp0;
                        AListVisitablePointable listPointable = (AListVisitablePointable) vp1;

                        try {
                            addFields(recordPointable, listPointable);

                            recordBuilder.write(output.getDataOutput(), true);
                        } catch (IOException | AsterixException e) {
                            throw new AlgebricksException(e);
                        }
                    }

                    private void addFields(ARecordVisitablePointable inputRecordPointer,
                            AListVisitablePointable listPointable) throws IOException, AsterixException,
                            AlgebricksException {

                        List<IVisitablePointable> inputFields;
                        List<IVisitablePointable> names;
                        List<IVisitablePointable> values;
                        IVisitablePointable namePointable = null;
                        IVisitablePointable valuePointable = null;

                        // Add original record without duplicate checking
                        for (int i = 0; i < inputRecordPointer.getFieldNames().size(); ++i) {
                            IVisitablePointable fnp = inputRecordPointer.getFieldNames().get(i);
                            IVisitablePointable fvp = inputRecordPointer.getFieldValues().get(i);
                            pu.addField(recordBuilder, fnp, fvp);
                        }

                        // Get the fields from a list of record
                        inputFields = listPointable.getItems();


                        for(IVisitablePointable fieldRecPointer: inputFields)  {
                            if(!PointableUtils.isType(ATypeTag.RECORD, fieldRecPointer)) {
                                throw new AlgebricksException("Expected list of record, got " +
                                        PointableUtils.getTypeTag(fieldRecPointer));
                            }

                            names = ((ARecordVisitablePointable)fieldRecPointer).getFieldNames();
                            values = ((ARecordVisitablePointable)fieldRecPointer).getFieldValues();

                            // Get name and value of the field to be added
                            // Use loop to account for the cases where users switches the order of the fields
                            IVisitablePointable fieldName;
                            for(int j=0; j<names.size(); j++) {
                                fieldName = names.get(j);
                                // if fieldName is "field-name" then read the name
                                if (PointableUtils.byteArrayEqual(fieldNamePointer, fieldName)) {
                                    namePointable = values.get(j);
                                } else { // otherwise the fieldName is "field-value". Thus, read the value
                                    valuePointable = values.get(j);
                                }
                            }

                            int pos = recType.findFieldPosition(namePointable.getByteArray(),
                                    namePointable.getStartOffset()+1, namePointable.getLength()-1);
                            if (pos>-1) {
                                if(namePointable != null && valuePointable != null) {
                                    pu.addField(recordBuilder, namePointable, valuePointable);
                                }
                            } else {
                                throw new AlgebricksException("Conflicting duplicate field found.");
                            } // else do nothing (keep the current value)
                        }
                    }

                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.ADD_FIELDS;
    }
}
