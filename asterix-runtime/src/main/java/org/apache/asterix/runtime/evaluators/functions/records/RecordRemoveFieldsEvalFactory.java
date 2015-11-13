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
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.functions.PointableUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

class RecordRemoveFieldsEvalFactory implements ICopyEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    private static final byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    private static final byte SER_ORDEREDLIST_TYPE_TAG = ATypeTag.ORDEREDLIST.serialize();
    
    private ICopyEvaluatorFactory inputRecordEvalFactory;
    private ICopyEvaluatorFactory removeFieldPathsFactory;
    private ARecordType reqRecType;
    private ARecordType inputRecType;
    private AOrderedListType inputListType;

    private final Deque<IVisitablePointable> recordPath = new ArrayDeque<>();

    public RecordRemoveFieldsEvalFactory(ICopyEvaluatorFactory inputRecordEvalFactory,
            ICopyEvaluatorFactory removeFieldPathsFactory, ARecordType reqRecType,
            ARecordType inputRecType,  AOrderedListType inputListType) {
        this.inputRecordEvalFactory = inputRecordEvalFactory;
        this.removeFieldPathsFactory = removeFieldPathsFactory;
        this.reqRecType = reqRecType;
        this.inputRecType = inputRecType;
        this.inputListType = inputListType;

    }


    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ANull> nullSerDe = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ANULL);


    @Override
    public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
        final ARecordType cashedRecType;

        try {
            cashedRecType = new ARecordType(reqRecType.getTypeName(),
                    reqRecType.getFieldNames(), reqRecType.getFieldTypes(),
                    reqRecType.isOpen());
        } catch (AsterixException | HyracksDataException e) {
            throw new IllegalStateException();
        }

        final PointableAllocator pa = new PointableAllocator();
        final IVisitablePointable vp0 = pa.allocateRecordValue(inputRecType);
        final IVisitablePointable vp1 = pa.allocateListValue(inputListType);
        final ArrayBackedValueStorage outInput0 = new ArrayBackedValueStorage();
        final ArrayBackedValueStorage outInput1 = new ArrayBackedValueStorage();
        final ICopyEvaluator eval0 = inputRecordEvalFactory.createEvaluator(outInput0);
        final ICopyEvaluator eval1 = removeFieldPathsFactory.createEvaluator(outInput1);

        final List<RecordBuilder> rbStack = new ArrayList<>();
        final ArrayBackedValueStorage tabvs = new ArrayBackedValueStorage();

        final PointableUtils pu = new PointableUtils();

        return new ICopyEvaluator() {

            private boolean isValidPath(AListVisitablePointable inputList) throws HyracksDataException {
                List<IVisitablePointable> items = inputList.getItems();
                List<IVisitablePointable> typeTags = inputList.getItemTags();

                int pathLen = recordPath.size();
                for (int i=0; i<items.size(); i++) {
                    IVisitablePointable item = items.get(i);
                    if (PointableUtils.isType(ATypeTag.ORDEREDLIST, typeTags.get(i))) {
                        List<IVisitablePointable> inputPathItems = ((AListVisitablePointable)item).getItems();

                        if(pathLen == inputPathItems.size()) {
                            boolean match = true;
                            Iterator<IVisitablePointable> fpi = recordPath.iterator();
                            for (int j = inputPathItems.size() - 1; j >= 0; j--) {
                                IVisitablePointable fnvp = fpi.next();
                                IVisitablePointable pathElement = inputPathItems.get(j);

                                match &= PointableUtils.isEqual(pathElement, fnvp);

                                if (!match)
                                    break;
                            }
                            if (match)
                                return false; // Not a valid path for the output record
                        }
                    } else {
                        if (PointableUtils.isEqual(recordPath.getFirst(), item)) {
                            return false;
                        }
                    }
                }
                return true;

            }

            @Override
            public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                outInput0.reset();
                outInput1.reset();

                eval0.evaluate(tuple);
                eval1.evaluate(tuple);

                if (outInput0.getByteArray()[0] == SER_NULL_TYPE_TAG) {
                    try {
                        nullSerDe.serialize(ANull.NULL, output.getDataOutput());
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                    return;
                }

                vp0.set(outInput0);
                vp1.set(outInput1);

                byte[] listBytes = outInput1.getByteArray();
                if (listBytes[0] != SER_ORDEREDLIST_TYPE_TAG) {
                    throw new AlgebricksException(AsterixBuiltinFunctions.REMOVE_FIELDS.getName()
                            + ": expects input type ORDEREDLIST, but got "
                            + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(listBytes[0]));
                }

                ARecordVisitablePointable recordPointable = (ARecordVisitablePointable) vp0;
                AListVisitablePointable listPointable = (AListVisitablePointable) vp1;

                try {
                    recordPath.clear();
                    rbStack.clear();
                    processRecord(cashedRecType, recordPointable, listPointable, 0);
                    rbStack.get(0).write(output.getDataOutput(), true);
                } catch (IOException | AsterixException e) {
                    throw new AlgebricksException(e);
                }
            }

            private void addKeptFieldToSubRecord(ARecordType subRecordType, IVisitablePointable fieldNamePointable,
                    IVisitablePointable fieldValuePointable, IVisitablePointable fieldTypePointable,
                    AListVisitablePointable inputList, int nestedLevel)
                    throws IOException, AsterixException, AlgebricksException {

                String fieldName = pu.getFieldName(fieldNamePointable);

                ARecordType resType = subRecordType != null ? subRecordType :
                        DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;

                if (resType.isClosedField(fieldName)) {
                    int pos = resType.findFieldPosition(fieldName);
                    if (PointableUtils.isType(ATypeTag.RECORD, fieldTypePointable)) {
                        processRecord((ARecordType) resType.getFieldType(fieldName),
                                (ARecordVisitablePointable) fieldValuePointable, inputList, nestedLevel + 1);
                        tabvs.reset();
                        rbStack.get(nestedLevel + 1).write(tabvs.getDataOutput(), true);
                        rbStack.get(nestedLevel).addField(pos, tabvs);
                    } else {
                        rbStack.get(nestedLevel).addField(pos, fieldValuePointable);
                    }
                } else {
                    if (PointableUtils.isType(ATypeTag.RECORD, fieldTypePointable)) {
                        processRecord((ARecordType) resType.getFieldType(fieldName),
                                (ARecordVisitablePointable) fieldValuePointable, inputList, nestedLevel + 1);
                        tabvs.reset();
                        rbStack.get(nestedLevel + 1).write(tabvs.getDataOutput(), true);
                        rbStack.get(nestedLevel).addField(fieldNamePointable, tabvs);
                    } else {
                        rbStack.get(nestedLevel).addField(fieldNamePointable, fieldValuePointable);
                    }
                }
            }



            private void processRecord(ARecordType resType, ARecordVisitablePointable srp, AListVisitablePointable inputList, int nestedLevel)
                    throws IOException, AsterixException, AlgebricksException {
                if (rbStack.size() < (nestedLevel + 1)) {
                    rbStack.add(new RecordBuilder());
                }

                rbStack.get(nestedLevel).reset(resType);
                rbStack.get(nestedLevel).init();

                List<IVisitablePointable> fieldNames = srp.getFieldNames();
                List<IVisitablePointable> fieldValues = srp.getFieldValues();
                List<IVisitablePointable> fieldTypes = srp.getFieldTypeTags();

                for(int i=0; i<fieldNames.size(); i++) {
                    IVisitablePointable subRecFieldName = fieldNames.get(i);
                    recordPath.push(subRecFieldName);
                    if(isValidPath(inputList)) {
                        addKeptFieldToSubRecord(resType, subRecFieldName, fieldValues.get(i), fieldTypes.get(i),
                                inputList, nestedLevel);
                    }
                    recordPath.pop();
                }
            }

        };
    }
}

