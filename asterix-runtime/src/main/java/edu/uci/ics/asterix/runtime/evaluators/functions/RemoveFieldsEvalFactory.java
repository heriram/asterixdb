package edu.uci.ics.asterix.runtime.evaluators.functions;

import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.pointables.AListPointable;
import edu.uci.ics.asterix.om.pointables.ARecordPointable;
import edu.uci.ics.asterix.om.pointables.PointableAllocator;
import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.om.types.*;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class RemoveFieldsEvalFactory implements ICopyEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    private static final byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    private static final byte SER_ORDEREDLIST_TYPE_TAG = ATypeTag.ORDEREDLIST.serialize();

    private static final int STRING_VALUE_OFFSET = 3;

    private ICopyEvaluatorFactory inputRecordEvalFactory;
    private ICopyEvaluatorFactory removeFieldPathsFactory;
    private ARecordType outputRecordType;
    private ARecordType inputRecType;
    private AOrderedListType inputListType;

    private final Deque<IVisitablePointable> recordPath = new ArrayDeque<>();

    public RemoveFieldsEvalFactory(ICopyEvaluatorFactory inputRecordEvalFactory,
            ICopyEvaluatorFactory removeFieldPathsFactory, ARecordType outputRecordType,
            ARecordType inputRecType,  AOrderedListType inputListType) {
        this.inputRecordEvalFactory = inputRecordEvalFactory;
        this.removeFieldPathsFactory = removeFieldPathsFactory;
        this.outputRecordType = outputRecordType;
        this.inputRecType = inputRecType;
        this.inputListType = inputListType;

    }


    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ANull> nullSerDe = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ANULL);


    @Override
    public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
        final ARecordType resultRecordType;

        try {
            resultRecordType = new ARecordType(outputRecordType.getTypeName(),
                    outputRecordType.getFieldNames(), outputRecordType.getFieldTypes(),
                    outputRecordType.isOpen());
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


        // For deserialization purposes
        final ByteArrayAccessibleOutputStream nameOutputStream = new ByteArrayAccessibleOutputStream();
        final ByteArrayInputStream namebais = new ByteArrayInputStream(nameOutputStream.getByteArray());
        final DataInputStream namedis = new DataInputStream(namebais);

        final DataOutput out = output.getDataOutput();

        final List<RecordBuilder> rbStack = new ArrayList<>();
        final ArrayBackedValueStorage tabvs = new ArrayBackedValueStorage();

        final RecordBuilder recordBuilder = new RecordBuilder();
        recordBuilder.reset(resultRecordType);


        return new ICopyEvaluator() {
            private final UTF8StringPointable usp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();


            private boolean byteSubArraysEquals(byte[] ba1, int s1, int l1, byte[] ba2, int s2, int l2) {
                if (ba1==ba2) return true;
                if (l1 != l2) return false;
                int i1=s1;
                int i2=s2;
                for(int i=0; i<l1; i++) {
                    if (ba1[i1]!=ba2[i2]) return false;
                    i1++;
                    i2++;
                }

                return true;
            }

            private boolean isValidField(IVisitablePointable fieldNamePointable, AListPointable inputList) throws AsterixException {
                List<IVisitablePointable> items = inputList.getItems();
                List<IVisitablePointable> typeTags = inputList.getItemTags();

                for(int i=0; i<items.size(); i++){
                    IVisitablePointable item = items.get(i);
                    IVisitablePointable itemTag = typeTags.get(i);

                    if(isType(ATypeTag.STRING, itemTag)) {
                        if(byteSubArraysEquals(fieldNamePointable.getByteArray(), fieldNamePointable.getStartOffset()+STRING_VALUE_OFFSET,
                                fieldNamePointable.getLength()-STRING_VALUE_OFFSET, item.getByteArray(), item.getStartOffset()+STRING_VALUE_OFFSET,
                                item.getLength()-STRING_VALUE_OFFSET)) {
                            return false;
                        }
                    } else if (isType(ATypeTag.ORDEREDLIST, itemTag)) {
                        List<IVisitablePointable> path = ((AListPointable)item).getItems();
                        if(path.size()==1 && byteSubArraysEquals(fieldNamePointable.getByteArray(),
                                fieldNamePointable.getStartOffset()+STRING_VALUE_OFFSET, fieldNamePointable.getLength()-STRING_VALUE_OFFSET,
                                item.getByteArray(), item.getStartOffset()+STRING_VALUE_OFFSET, item.getLength()-STRING_VALUE_OFFSET)) {
                            return false;
                        }
                    }
                }

                return true;
            }


            private boolean isValidPath(AListPointable inputList) {
                List<IVisitablePointable> items = inputList.getItems();
                List<IVisitablePointable> typeTags = inputList.getItemTags();

                int pathLen = recordPath.size();
                for (int i=0; i<items.size(); i++) {
                    IVisitablePointable item = items.get(i);
                    if (isType(ATypeTag.ORDEREDLIST, typeTags.get(i))) {
                        List<IVisitablePointable> inputPathItems = ((AListPointable)item).getItems();

                        // Ignore input paths longer than current level
                        if(pathLen != inputPathItems.size()) {
                            return true;
                        }
                        boolean match = true;
                        Iterator<IVisitablePointable> fpi = recordPath.iterator();
                        for(int j=inputPathItems.size()-1; j>=0; j--) {
                            IVisitablePointable fnvp= fpi.next();
                            IVisitablePointable pathElement = inputPathItems.get(j);
                            /** TODO REMOVE ***/
                            String s1 = new String(pathElement.getByteArray(), pathElement.getStartOffset()+STRING_VALUE_OFFSET, pathElement.getLength()-STRING_VALUE_OFFSET);
                            String s2 = new String(fnvp.getByteArray(), fnvp.getStartOffset()+STRING_VALUE_OFFSET, fnvp.getLength()-STRING_VALUE_OFFSET);
                            /*************/
                            match &= byteSubArraysEquals(pathElement.getByteArray(),
                                    pathElement.getStartOffset()+STRING_VALUE_OFFSET,
                                    pathElement.getLength()-STRING_VALUE_OFFSET,
                                    fnvp.getByteArray(), fnvp.getStartOffset()+STRING_VALUE_OFFSET,
                                    fnvp.getLength()-STRING_VALUE_OFFSET);

                            if (!match) break;
                        }
                        if (match) return false; // Not a valid path for the output record

                    } else { // TODO Check - assuming AString
                        if(byteSubArraysEquals(recordPath.getFirst().getByteArray(),
                                recordPath.getFirst().getStartOffset() + STRING_VALUE_OFFSET,
                                recordPath.getFirst().getLength() - STRING_VALUE_OFFSET, item.getByteArray(),
                                item.getStartOffset() + STRING_VALUE_OFFSET, item.getLength() - STRING_VALUE_OFFSET)) {
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

                ARecordPointable recordPointable = (ARecordPointable) vp0;
                AListPointable listPointable = (AListPointable) vp1;

                try {
                    recordPath.clear();
                    removeNestedFields(outputRecordType, recordPointable, listPointable, 0);
                    //removeFieldsFromRecords(outputRecordType, recordPointable, listPointable);
                    rbStack.get(0).write(output.getDataOutput(), true);
                    //recordBuilder.write(output.getDataOutput(), true);
                } catch (IOException | AsterixException e) {
                    throw new AlgebricksException(e);
                }
            }

            private String getFieldName(IVisitablePointable fieldNamePointable) throws IOException {
                nameOutputStream.reset();
                nameOutputStream.write(fieldNamePointable.getByteArray(),
                        fieldNamePointable.getStartOffset() + 1, fieldNamePointable.getLength());
                namedis.reset();

                return AStringSerializerDeserializer.INSTANCE.deserialize(namedis).getStringValue();
            }


            private void addFieldToSubRecord(ARecordType resType, IVisitablePointable fieldNamePointable,
                    IVisitablePointable fieldValuePointable, IVisitablePointable fieldTypePointable,
                    AListPointable inputList, int nestedLevel)
                    throws IOException, AsterixException, AlgebricksException {

                String fieldName = getFieldName(fieldNamePointable);

                if (resType.isClosedField(fieldName)) {
                    int pos = resType.findFieldPosition(fieldName);
                    if (isType(ATypeTag.RECORD, fieldTypePointable)) {
                        removeNestedFields((ARecordType) resType.getFieldType(fieldName),
                                (ARecordPointable)fieldValuePointable, inputList, nestedLevel + 1);
                        tabvs.reset();
                        rbStack.get(nestedLevel + 1).write(tabvs.getDataOutput(), true);
                        rbStack.get(nestedLevel).addField(pos, tabvs);
                    } else {
                        rbStack.get(nestedLevel).addField(pos, fieldValuePointable);
                    }
                } else {
                    if (isType(ATypeTag.RECORD, fieldTypePointable)) {
                        removeNestedFields((ARecordType) resType.getFieldType(fieldName),
                                (ARecordPointable) fieldValuePointable, inputList, nestedLevel + 1);

                        tabvs.reset();
                        rbStack.get(nestedLevel + 1).write(tabvs.getDataOutput(), true);
                        rbStack.get(nestedLevel).addField(fieldNamePointable, tabvs);
                    } else {
                        rbStack.get(nestedLevel).addField(fieldNamePointable, fieldValuePointable);
                    }
                }
            }


            private boolean isType(ATypeTag typeTag, IVisitablePointable visitablePointable) {
                byte[] bytes = visitablePointable.getByteArray();
                int s = visitablePointable.getStartOffset();
                ATypeTag tt = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[s]);
                return (tt==typeTag);
            }

            private void removeFieldsFromRecords(ARecordType resType, ARecordPointable inputRecord, AListPointable inputList) throws IOException,
                    AsterixException, AlgebricksException {
                recordPath.clear();

                for (int i = 0; i < inputRecord.getFieldNames().size(); i++) {
                    IVisitablePointable namePointable = inputRecord.getFieldNames().get(i);
                    IVisitablePointable valuePointable = inputRecord.getFieldValues().get(i);
                    IVisitablePointable typePointable = inputRecord.getFieldTypeTags().get(i);

                    if(isValidField(namePointable, inputList)) {
                        if(isType(ATypeTag.RECORD, typePointable)) {
                            recordPath.push(namePointable);
                            removeNestedFields((ARecordType)resType.getFieldType(getFieldName(namePointable)),
                                    (ARecordPointable)valuePointable, inputList, 1);
                            recordPath.pop();
                        } else {
                            rbStack.add(new RecordBuilder());
                            RecordBuilder rb = rbStack.get(0);
                            rb.reset(resType);
                            rb.init();
                            addFieldToSubRecord(resType, namePointable, valuePointable, typePointable, inputList, 0);
                        }
                    }
                }

            }

            private void removeNestedFields(ARecordType resType, ARecordPointable srp, AListPointable inputList, int nestedLevel)
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
                        addFieldToSubRecord(resType, subRecFieldName, fieldValues.get(i), fieldTypes.get(i),
                                inputList, nestedLevel);
                    }
                    recordPath.pop();
                }
            }

        };
    }
}

