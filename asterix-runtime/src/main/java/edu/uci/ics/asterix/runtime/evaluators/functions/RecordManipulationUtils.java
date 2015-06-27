package edu.uci.ics.asterix.runtime.evaluators.functions;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.pointables.ARecordPointable;
import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.api.IMutableValueStorage;
import edu.uci.ics.hyracks.data.std.api.IValueReference;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

public class RecordManipulationUtils {
    static final ByteArrayAccessibleOutputStream outputStream = new ByteArrayAccessibleOutputStream();
    static final DataInputStream dis = new DataInputStream(
            new ByteArrayInputStream(outputStream.getByteArray()));

    public static final RecordManipulationUtils INSTANCE = new RecordManipulationUtils();

    private static final byte SER_ORDEREDLIST_TYPE_TAG = ATypeTag.ORDEREDLIST.serialize();
    private static final byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();

    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ANull> nullSerDe = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ANULL);

    private final IBinaryComparator fieldNameComparator = PointableBinaryComparatorFactory
            .of(UTF8StringPointable.FACTORY).createBinaryComparator();

    private final ArrayBackedValueStorage stringBuffer = new ArrayBackedValueStorage();

    private RecordManipulationUtils(){
    }

    public boolean compare(IValueReference a, IValueReference b) throws HyracksDataException {
        // start+1 and len-1 due to the type tag
        String s1 = new String(a.getByteArray(), a.getStartOffset() + 1, a.getLength() - 1);
        String s2 = new String(b.getByteArray(), b.getStartOffset() + 1, b.getLength() - 1);

        return (fieldNameComparator.compare(a.getByteArray(), a.getStartOffset() + 1, a.getLength() - 1,
                b.getByteArray(), b.getStartOffset() + 1, b.getLength() - 1)==0);
    }

    public boolean byteArrayEqual(IValueReference valueRef1, IValueReference valueRef2) throws HyracksDataException {
        if (valueRef1 == null || valueRef2 == null) return false;
        if (valueRef1 == valueRef2) return true;

        int length1 = valueRef1.getLength();
        int length2 = valueRef2.getLength();

        if (length1 != length2) return false;

        byte[] bytes1 = valueRef1.getByteArray();
        byte[] bytes2 = valueRef2.getByteArray();
        int start1 = valueRef1.getStartOffset()+3;
        int start2 = valueRef2.getStartOffset()+3;

        int end = start1+length1-3;

        for (int i=start1, j=start2; i<end; i++,j++) {
            if (bytes1[i] != bytes2[j]) return false;
        }

        return true;
    }

    public String getFieldName(IValueReference fieldNamePointable) throws IOException {
        outputStream.reset();
        outputStream.write(fieldNamePointable.getByteArray(),
                fieldNamePointable.getStartOffset() + 1, fieldNamePointable.getLength());
        dis.reset();

        return AStringSerializerDeserializer.INSTANCE.deserialize(dis).getStringValue();
    }

    public boolean isType(ATypeTag typeTag, IVisitablePointable visitablePointable) {
        return (getTypeTag(visitablePointable)==typeTag);
    }

    public ATypeTag getTypeTag(IVisitablePointable visitablePointable) {
        byte[] bytes = visitablePointable.getByteArray();
        int s = visitablePointable.getStartOffset();
        return EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[s]);
    }

    public void validateInputList(byte typeByte) throws AlgebricksException {
        if (typeByte != SER_ORDEREDLIST_TYPE_TAG) {
            throw new AlgebricksException(AsterixBuiltinFunctions.REMOVE_FIELDS.getName()
                    + ": expects input type ORDEREDLIST, but got "
                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(typeByte));
        }
    }

    public boolean isNullRecord(IMutableValueStorage abvs, DataOutput output) throws AlgebricksException {
        if (abvs.getByteArray()[0] == SER_NULL_TYPE_TAG) {
            try {
                nullSerDe.serialize(ANull.NULL, output);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
            }
            return true;
        }

        return false;
    }

    public boolean checkConflict(IVisitablePointable fieldNamePointable, ARecordPointable recordPointerRight) {
        for (int i = 0; i < recordPointerRight.getFieldNames().size(); ++i) {
            IVisitablePointable fp = recordPointerRight.getFieldNames().get(i);
            if (fp.equals(fieldNamePointable)) {
                return true;
            }
        }
        return false;
    }

    public UTF8StringPointable serializeString(String str) throws AlgebricksException {
        UTF8StringPointable fnp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        stringBuffer.reset();
        try {
            UTF8StringSerializerDeserializer.INSTANCE.serialize(str, stringBuffer.getDataOutput());
        } catch (HyracksDataException e) {
            throw new AlgebricksException("Could not serialize " + str);
        }
        fnp.set(stringBuffer);
        return fnp;
    }
}
