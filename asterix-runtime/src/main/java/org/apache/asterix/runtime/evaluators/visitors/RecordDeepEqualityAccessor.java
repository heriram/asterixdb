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
package org.apache.asterix.runtime.evaluators.visitors;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.functions.BinaryHashMap;
import org.apache.asterix.runtime.evaluators.functions.PointableUtils;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;

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

        // Build phase: Add items into hash map, starting with first record.
        for (int i = 0; i<s0; i++) {
            IVisitablePointable fieldName = fieldNames0.get(i);
            keyEntry.set(fieldName.getByteArray(), fieldName.getStartOffset(), fieldName.getLength());
            IntegerPointable.setInteger(valEntry.buf, 0, i);
            hashMap.put(keyEntry, valEntry);
        }

        return compareValues(rec0.getFieldTypeTags(), rec0.getFieldValues(),
                fieldNames1, rec1.getFieldTypeTags(), rec1.getFieldValues());
    }


    private boolean compareValues(List<IVisitablePointable> fieldTypes0, List<IVisitablePointable> fieldValues0,
            List<IVisitablePointable> fieldNames1, List<IVisitablePointable> fieldTypes1,
            List<IVisitablePointable> fieldValues1) throws HyracksDataException, AsterixException {

        // Probe phase: Probe items from second record
        Pair<IVisitablePointable, Boolean> arg=null;
        for(int i=0; i<fieldNames1.size(); i++) {
            IVisitablePointable fieldName = fieldNames1.get(i);
            keyEntry.set(fieldName.getByteArray(), fieldName.getStartOffset(), fieldName.getLength());
            BinaryHashMap.BinaryEntry entry = hashMap.get(keyEntry);
            if (entry == null) {
                return false;
            }

            int fieldId0 = IntegerPointable.getInteger(entry.buf, entry.off);
            ATypeTag fieldType0 = PointableUtils.getTypeTag(fieldTypes0.get(fieldId0));
            if(fieldType0.isDerivedType() && fieldType0 != PointableUtils.getTypeTag(fieldTypes1.get(i))) {
                return false;
            }
            arg = new Pair<IVisitablePointable, Boolean>(fieldValues1.get(i), false);
            switch (fieldType0) {
                case ORDEREDLIST:
                case UNORDEREDLIST:
                    ((AListVisitablePointable)fieldValues0.get(fieldId0)).accept(visitor, arg);
                    break;
                case RECORD:
                    ((ARecordVisitablePointable)fieldValues0.get(fieldId0)).accept(visitor, arg);
                    break;
                case ANY:
                    return false;
                // TODO Should have a way to check "ANY" types too
                default:
                    ((AFlatValuePointable)fieldValues0.get(fieldId0)).accept(visitor, arg);
            }

            if (!arg.second) return false;
        }
        return true;
    }
}
