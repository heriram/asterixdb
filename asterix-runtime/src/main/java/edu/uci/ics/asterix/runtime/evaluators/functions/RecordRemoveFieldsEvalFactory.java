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
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.api.IValueReference;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

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


        // For deserialization purposes
        final ByteArrayAccessibleOutputStream nameOutputStream = new ByteArrayAccessibleOutputStream();
        final ByteArrayInputStream namebais = new ByteArrayInputStream(nameOutputStream.getByteArray());
        final DataInputStream namedis = new DataInputStream(namebais);

        final DataOutput out = output.getDataOutput();

        final List<RecordBuilder> rbStack = new ArrayList<>();
        final ArrayBackedValueStorage tabvs = new ArrayBackedValueStorage();

        final IBinaryComparator fieldNameComparator = PointableBinaryComparatorFactory
                .of(UTF8StringPointable.FACTORY).createBinaryComparator();

        return new ICopyEvaluator() {

            private boolean compare(IValueReference a, IValueReference b) throws HyracksDataException {
                // start+1 and len-1 due to the type tag
                return (fieldNameComparator.compare(a.getByteArray(), a.getStartOffset() + 1, a.getLength() - 1,
                        b.getByteArray(), b.getStartOffset() + 1, b.getLength() - 1)==0);
            }

            private boolean isValidPath(AListPointable inputList) throws HyracksDataException {
                List<IVisitablePointable> items = inputList.getItems();
                List<IVisitablePointable> typeTags = inputList.getItemTags();

                int pathLen = recordPath.size();
                for (int i=0; i<items.size(); i++) {
                    IVisitablePointable item = items.get(i);
                    if (isType(ATypeTag.ORDEREDLIST, typeTags.get(i))) {
                        List<IVisitablePointable> inputPathItems = ((AListPointable)item).getItems();

                        if(pathLen == inputPathItems.size()) {
                            boolean match = true;
                            Iterator<IVisitablePointable> fpi = recordPath.iterator();
                            for (int j = inputPathItems.size() - 1; j >= 0; j--) {
                                IVisitablePointable fnvp = fpi.next();
                                IVisitablePointable pathElement = inputPathItems.get(j);

                                match &= compare(pathElement, fnvp);

                                if (!match)
                                    break;
                            }
                            if (match)
                                return false; // Not a valid path for the output record
                        }
                    } else {
                        if (compare(recordPath.getFirst(), item)) {
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
                    rbStack.clear();
                    processRecord(cashedRecType, recordPointable, listPointable, 0);
                    rbStack.get(0).write(output.getDataOutput(), true);
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
                        processRecord((ARecordType) resType.getFieldType(fieldName),
                                (ARecordPointable) fieldValuePointable, inputList, nestedLevel + 1);
                        tabvs.reset();
                        rbStack.get(nestedLevel + 1).write(tabvs.getDataOutput(), true);
                        rbStack.get(nestedLevel).addField(pos, tabvs);
                    } else {
                        rbStack.get(nestedLevel).addField(pos, fieldValuePointable);
                    }
                } else {
                    if (isType(ATypeTag.RECORD, fieldTypePointable)) {
                        processRecord((ARecordType) resType.getFieldType(fieldName),
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


            private void processRecord(ARecordType resType, ARecordPointable srp, AListPointable inputList, int nestedLevel)
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

