package edu.uci.ics.asterix.runtime.evaluators.visitors;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.comparators.ListItemBinaryComparatorFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.hash.ListItemBinaryHashFunctionFactory;
import edu.uci.ics.asterix.om.pointables.AFlatValuePointable;
import edu.uci.ics.asterix.om.pointables.AListPointable;
import edu.uci.ics.asterix.om.pointables.ARecordPointable;
import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.runtime.evaluators.functions.BinaryHashMap;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;

import java.util.Arrays;
import java.util.List;

class RecordDeepEqualityAccessor {
    private DeepEqualityVisitor visitor;

    private final int TABLE_SIZE = 100;
    private final int TABLE_FRAME_SIZE = 32768;

    private IBinaryHashFunction putHashFunc = ListItemBinaryHashFunctionFactory.INSTANCE.createBinaryHashFunction();
    private IBinaryHashFunction getHashFunc = ListItemBinaryHashFunctionFactory.INSTANCE.createBinaryHashFunction();
    private IBinaryComparator cmp = ListItemBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    private BinaryHashMap hashMap = new BinaryHashMap(TABLE_SIZE, TABLE_FRAME_SIZE, putHashFunc, getHashFunc, cmp);
    private BinaryHashMap.BinaryEntry keyEntry = new BinaryHashMap.BinaryEntry();
    private BinaryHashMap.BinaryEntry valEntry = new BinaryHashMap.BinaryEntry();

    public RecordDeepEqualityAccessor() {
        byte[] emptyValBuf = new byte[8];
        Arrays.fill(emptyValBuf, (byte) 0);
        valEntry.set(emptyValBuf, 0, 8);
    }

    public boolean accessRecord(IVisitablePointable recAccessor0, IVisitablePointable recAccessor1,
            DeepEqualityVisitor visitor) throws HyracksDataException, AsterixException {

        if (recAccessor0.equals(recAccessor1)) {
            return true;
        }

        this.visitor = visitor;

        hashMap.clear();

        ARecordPointable rec0 = (ARecordPointable) recAccessor0;
        List<IVisitablePointable> fieldNames0 = rec0.getFieldNames();

        ARecordPointable rec1 = (ARecordPointable) recAccessor1;
        List<IVisitablePointable> fieldNames1 = rec1.getFieldNames();

        int s0 = fieldNames0.size();
        int s1 = fieldNames1.size();
        if (s0 != s1)
            return false;

        // Build phase: Add items into hash map, starting with first list.
        for (int i = 0; i < s0; i++) {
            IVisitablePointable item = fieldNames0.get(i);
            byte[] buf = item.getByteArray();
            int off = item.getStartOffset();
            int len = item.getLength();
            keyEntry.set(buf, off, len);
            IntegerPointable.setInteger(valEntry.buf, 0, i);
            BinaryHashMap.BinaryEntry entry = hashMap.put(keyEntry, valEntry);
        }

        return compareValues(rec0.getFieldTypeTags(), rec0.getFieldValues(),
                fieldNames1, rec1.getFieldTypeTags(), rec1.getFieldValues());
    }


    private boolean compareValues(List<IVisitablePointable> fieldTypes0, List<IVisitablePointable> fieldValues0,
            List<IVisitablePointable> fieldNames1, List<IVisitablePointable> fieldTypes1,
            List<IVisitablePointable> fieldValues1) throws HyracksDataException, AsterixException {

        // Probe phase: Probe items from second list
        Pair<IVisitablePointable, Boolean> arg=null;
        for(int index1=0; index1<fieldNames1.size(); index1++) {
            IVisitablePointable item = fieldNames1.get(index1);
            byte[] buf = item.getByteArray();
            int off = item.getStartOffset();
            int len = item.getLength();
            keyEntry.set(buf, off, len);
            BinaryHashMap.BinaryEntry entry = hashMap.get(keyEntry);
            if (entry == null) {
                return false;
            }

            int index0 = IntegerPointable.getInteger(entry.buf, entry.off);
            ATypeTag fieldType0 = visitor.getTypeTag(fieldTypes0.get(index0));
            if(fieldType0 != visitor.getTypeTag(fieldTypes1.get(index1))) {
                return false;
            }

            arg = new Pair<IVisitablePointable, Boolean>(fieldValues1.get(index1), false);
            switch (fieldType0) {
                case ORDEREDLIST:
                case UNORDEREDLIST:
                    ((AListPointable)fieldValues0.get(index0)).accept(visitor, arg);
                    break;
                case RECORD:
                    ((ARecordPointable)fieldValues0.get(index0)).accept(visitor, arg);
                    break;
                case ANY:
                    return false;
                // TODO Should have a way to check "ANY" types too
                default:
                    ((AFlatValuePointable)fieldValues0.get(index0)).accept(visitor, arg);
            }

            if (!arg.second) return false;
        }
        return true;
    }
}
