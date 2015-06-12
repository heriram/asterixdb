package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONException;

import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.pointables.ARecordPointable;
import edu.uci.ics.asterix.om.pointables.PointableAllocator;
import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.om.typecomputer.impl.RecordAppendTypeComputer;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.common.AsterixListAccessor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class RemoveFieldsDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    private static final byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    private static final byte SER_ORDEREDLIST_TYPE_TAG = ATypeTag.ORDEREDLIST.serialize();
    private static final byte SER_UNORDEREDLIST_TYPE_TAG = ATypeTag.UNORDEREDLIST.serialize();

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new RemoveFieldsDescriptor();
        }
    };

    private ARecordType outputRecordType;
    private ARecordType inputRecType;
    private AOrderedListType inputOrderedListType;

    public void reset(IAType outType, IAType inType, IAType inputListType) {
        outputRecordType = (ARecordType)outType;
        inputRecType = (ARecordType)inType;
        inputOrderedListType = (AOrderedListType)inputListType;
    }

    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {
            
            @SuppressWarnings("unchecked")
            private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.ANULL);

            
            private static final long serialVersionUID = 1L;

            final Set<UTF8StringPointable> removeFieldSet = new HashSet<UTF8StringPointable>();

            @SuppressWarnings("unchecked")
            private final ISerializerDeserializer<ANull> nullSerDe = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.ANULL);

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                final ARecordType reducedRecordType;

                try {
                    reducedRecordType = new ARecordType(outputRecordType.getTypeName(),
                            outputRecordType.getFieldNames(), outputRecordType.getFieldTypes(),
                            outputRecordType.isOpen());
                } catch (AsterixException | HyracksDataException e) {
                    throw new IllegalStateException();
                }
                
                final PointableAllocator pointableAllocator = new PointableAllocator();
                final IVisitablePointable visitablePointable = pointableAllocator.allocateRecordValue(inputRecType);
                final ArrayBackedValueStorage inputRecordValueStorage = new ArrayBackedValueStorage();
                final ICopyEvaluator evalRecord = args[0].createEvaluator(inputRecordValueStorage);
                
                final AsterixListAccessor listAccessor = new AsterixListAccessor();
                final ArrayBackedValueStorage inputListValueStorage = new ArrayBackedValueStorage();
                final ICopyEvaluator evalList = args[1].createEvaluator(inputListValueStorage);


                // For serialization purposes
                final ByteArrayAccessibleOutputStream nameOutputStream = new ByteArrayAccessibleOutputStream();
                final ByteArrayInputStream namebais = new ByteArrayInputStream(nameOutputStream.getByteArray());
                final DataInputStream namedis = new DataInputStream(namebais);

                final DataOutput out = output.getDataOutput();

                final RecordBuilder recordBuilder = new RecordBuilder();
                recordBuilder.reset(reducedRecordType);

                final UTF8StringPointable usp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
                removeFieldSet.clear();

                return new ICopyEvaluator() {

                    private boolean isRemoveField(IVisitablePointable fieldNamePointable) throws AsterixException,
                            HyracksDataException, AlgebricksException {
                        for (int i = 0; i < listAccessor.size(); i++) {
                            int itemOffset = listAccessor.getItemOffset(i);
                            ATypeTag itemType = listAccessor.getItemType(itemOffset);
                            if (itemType != ATypeTag.STRING) {
                                if (itemType == ATypeTag.NULL) {
                                    nullSerde.serialize(ANull.NULL, out);
                                    return false;
                                }
                                throw new AlgebricksException(AsterixBuiltinFunctions.STRING_CONCAT.getName()
                                        + ": expects type STRING/NULL for the list item but got " + itemType);
                            }

                            int length = listAccessor.getItemLength(itemOffset);
                            usp.set(listAccessor.getByteArray(), itemOffset, length);

                            if (usp.compareTo(fieldNamePointable.getByteArray(), fieldNamePointable.getStartOffset() + 1,
                                    fieldNamePointable.getLength() - 1)==0)
                                return true;

                        }
                        return false;
                    }

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        inputRecordValueStorage.reset();
                        inputListValueStorage.reset();
                        recordBuilder.init();

                        evalRecord.evaluate(tuple);
                        evalList.evaluate(tuple);

                        if (inputRecordValueStorage.getByteArray()[0] == SER_NULL_TYPE_TAG) {
                            try {
                                nullSerDe.serialize(ANull.NULL, output.getDataOutput());
                            } catch (HyracksDataException e) {
                                throw new AlgebricksException(e);
                            }
                            return;
                        }

                        byte[] listBytes = inputListValueStorage.getByteArray();

                        if (listBytes[0] != SER_ORDEREDLIST_TYPE_TAG) {
                            throw new AlgebricksException(AsterixBuiltinFunctions.REMOVE_FIELDS.getName()
                                    + ": expects input type ORDEREDLIST, but got "
                                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(listBytes[0]));
                        }

                        try {
                            listAccessor.reset(listBytes, 0);
                        } catch (AsterixException e) {
                            throw new AlgebricksException(e);
                        }

                        try {
                            for (int i = 0; i < listAccessor.size(); i++) {
                                int itemOffset;

                                itemOffset = listAccessor.getItemOffset(i);

                                ATypeTag itemType = listAccessor.getItemType(itemOffset);
                                if (itemType != ATypeTag.STRING) {
                                    if (itemType == ATypeTag.NULL) {
                                        nullSerde.serialize(ANull.NULL, out);
                                        return;
                                    }
                                    throw new AlgebricksException(AsterixBuiltinFunctions.STRING_CONCAT.getName()
                                            + ": expects type STRING/NULL for the list item but got " + itemType);
                                }
                                int length = listAccessor.getItemLength(itemOffset);
                                usp.set(listAccessor.getByteArray(), itemOffset + 1, length);

                                removeFieldSet.add(usp);

                            }
                        } catch (AsterixException | HyracksDataException e) {
                            throw new AlgebricksException(e);
                        }

                        visitablePointable.set(inputRecordValueStorage);

                        ARecordPointable recordPointable = (ARecordPointable) visitablePointable;

                        try {
                            removeFieldsFromRecords(recordPointable);
                            recordBuilder.write(output.getDataOutput(), true);
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

                    private void removeFieldsFromRecords(ARecordPointable inputRecord) throws IOException,
                            AsterixException, AlgebricksException {

                        // Add left record without duplicate checking
                        for (int i = 0; i < inputRecord.getFieldNames().size(); ++i) {
                            IVisitablePointable fieldNamePointable = inputRecord.getFieldNames().get(i);
                            IVisitablePointable fieldValuePointable = inputRecord.getFieldValues().get(i);
                            
                            if (!isRemoveField(fieldNamePointable)) {
                                String fieldName = getFieldName(fieldNamePointable);
                                if (reducedRecordType.isClosedField(fieldName)) {
                                    int position = reducedRecordType.findFieldPosition(fieldName);
                                    recordBuilder.addField(position, fieldValuePointable);
                                } else {
                                    recordBuilder.addField(fieldNamePointable, fieldValuePointable);
                                }
                            }
                        }
                    }

                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.REMOVE_FIELDS;
    }

}
