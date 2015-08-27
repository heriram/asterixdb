/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.runtime.evaluators.visitors;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.pointables.AFlatValuePointable;
import edu.uci.ics.asterix.om.pointables.AListVisitablePointable;
import edu.uci.ics.asterix.om.pointables.ARecordVisitablePointable;
import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.runtime.evaluators.functions.BinaryHashMap;
import edu.uci.ics.asterix.runtime.evaluators.functions.PointableUtils;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;

import java.util.List;

class RecordDeepEqualityAccessor {
    private DeepEqualityVisitor visitor;

    private BinaryHashMap.BinaryEntry keyEntry = new BinaryHashMap.BinaryEntry();
    private BinaryHashMap.BinaryEntry valEntry = new BinaryHashMap.BinaryEntry();
    private BinaryHashMap hashMap;

    public RecordDeepEqualityAccessor(int tableSize, int tableFrameSize) {
        hashMap = DeepEqualityVisitorUtils.initializeHashMap(tableSize, tableFrameSize, valEntry);
    }

    public RecordDeepEqualityAccessor() {
        hashMap = DeepEqualityVisitorUtils.initializeHashMap(valEntry);
    }

    public boolean accessRecord(IVisitablePointable recAccessor0, IVisitablePointable recAccessor1,
            DeepEqualityVisitor visitor) throws HyracksDataException, AsterixException {

        if (recAccessor0.equals(recAccessor1)) {
            return true;
        }

        this.visitor = visitor;

        hashMap.clear();

        ARecordVisitablePointable rec0 = (ARecordVisitablePointable) recAccessor0;
        List<IVisitablePointable> fieldNames0 = rec0.getFieldNames();

        ARecordVisitablePointable rec1 = (ARecordVisitablePointable) recAccessor1;
        List<IVisitablePointable> fieldNames1 = rec1.getFieldNames();

        int s0 = fieldNames0.size();
        int s1 = fieldNames1.size();
        if (s0 != s1)
            return false;

        // Build phase: Add items into hash map, starting with first list.
        for (int i = 0; i < s0; i++) {
            IVisitablePointable item = fieldNames0.get(i);
            keyEntry.set(item.getByteArray(), item.getStartOffset(), item.getLength());
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
            keyEntry.set(item.getByteArray(), item.getStartOffset(), item.getLength());
            BinaryHashMap.BinaryEntry entry = hashMap.get(keyEntry);
            if (entry == null) {
                return false;
            }

            int index0 = IntegerPointable.getInteger(entry.buf, entry.off);
            ATypeTag fieldType0 = PointableUtils.getTypeTag(fieldTypes0.get(index0));
            if(fieldType0 != PointableUtils.getTypeTag(fieldTypes1.get(index1))) {
                return false;
            }

            arg = new Pair<IVisitablePointable, Boolean>(fieldValues1.get(index1), false);
            switch (fieldType0) {
                case ORDEREDLIST:
                case UNORDEREDLIST:
                    ((AListVisitablePointable)fieldValues0.get(index0)).accept(visitor, arg);
                    break;
                case RECORD:
                    ((ARecordVisitablePointable)fieldValues0.get(index0)).accept(visitor, arg);
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
