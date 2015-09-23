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
import org.apache.asterix.runtime.evaluators.functions.BinaryHashMap.BinaryEntry;
import org.apache.asterix.runtime.evaluators.functions.PointableUtils;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;

import java.io.IOException;
import java.util.List;

class ListDeepEqualityAccessor {
    private DeepEqualityVisitor visitor;

    private BinaryHashMap hashMap;
    private BinaryEntry keyEntry = new BinaryEntry();
    private BinaryEntry valEntry = new BinaryEntry();

    public ListDeepEqualityAccessor() {
        hashMap = DeepEqualityVisitorUtils.initializeHashMap(valEntry);
    }

    public ListDeepEqualityAccessor(int tableSize, int tableFrameSize) {
        hashMap = DeepEqualityVisitorUtils.initializeHashMap(tableSize, tableFrameSize, valEntry);
    }

    public boolean accessList(IVisitablePointable listAccessor0, IVisitablePointable listAccessor1,
            DeepEqualityVisitor visitor) throws IOException, AsterixException {

        this.visitor = visitor;

        AListVisitablePointable list0 = (AListVisitablePointable)listAccessor0;
        List<IVisitablePointable> items0 = list0.getItems();
        List<IVisitablePointable> itemTagTypes0 = list0.getItemTags();


        AListVisitablePointable list1 = (AListVisitablePointable)listAccessor1;
        List<IVisitablePointable> items1 = list1.getItems();
        List<IVisitablePointable> itemTagTypes1 = list1.getItemTags();

        if (items0.size() != items1.size()) return false;

        boolean isOrdered1 = list0.ordered();
        if (isOrdered1 != list1.ordered())
            return false;

        if( isOrdered1) {
            return processOrderedList(items0, itemTagTypes0, items1, itemTagTypes1);
        } else {
            return processUnorderedList(items0, itemTagTypes0, items1, itemTagTypes1);
        }
    }

    private boolean compareListItems(ATypeTag fieldType0, IVisitablePointable item0, IVisitablePointable item1)
            throws AsterixException {
        Pair<IVisitablePointable, Boolean> arg = new Pair<IVisitablePointable, Boolean>(item1, false);
        switch (fieldType0) {
            case ORDEREDLIST:
            case UNORDEREDLIST:
                ((AListVisitablePointable)item0).accept(visitor, arg);
                break;
            case RECORD:
                ((ARecordVisitablePointable)item0).accept(visitor, arg);
                break;
            case ANY:
                return false;
            // TODO Should have a way to check "ANY" types too
            default:
                ((AFlatValuePointable)item0).accept(visitor, arg);
        }

        return arg.second;
    }

    private boolean processOrderedList(List<IVisitablePointable> items0, List<IVisitablePointable> itemTagTypes0,
            List<IVisitablePointable> items1, List<IVisitablePointable> itemTagTypes1)
            throws HyracksDataException, AsterixException {
        Pair<IVisitablePointable, Boolean> arg=null;
        for(int i=0; i<items0.size(); i++) {
            ATypeTag fieldType0 = PointableUtils.getTypeTag(itemTagTypes0.get(i));
            ATypeTag fieldType1 = PointableUtils.getTypeTag(itemTagTypes1.get(i));
            if(fieldType0 != fieldType1) {
                return false;
            }

            IVisitablePointable item1 = items1.get(i);
            if (!compareListItems(fieldType0, items0.get(i), items1.get(i)))
                return false;
        }

        return true;
    }

    private boolean processUnorderedList(List<IVisitablePointable> items0, List<IVisitablePointable> itemTagTypes0,
            List<IVisitablePointable> items1, List<IVisitablePointable> itemTagTypes1)
            throws HyracksDataException, AsterixException {

        hashMap.clear();
        // Build phase: Add items into hash map, starting with first list.
        for(int i=0; i<items0.size(); i++) {
            IVisitablePointable item = items0.get(i);
            byte[] buf = item.getByteArray();
            int off = item.getStartOffset();
            int len = item.getLength();
            keyEntry.set(buf, off, len);
            IntegerPointable.setInteger(valEntry.buf, 0, i);
            BinaryEntry entry = hashMap.put(keyEntry, valEntry);
        }

        return probeHashMap(items0, itemTagTypes0, items1, itemTagTypes1);
    }


    private boolean probeHashMap(List<IVisitablePointable> items0, List<IVisitablePointable> itemTagTypes0,
            List<IVisitablePointable> items1, List<IVisitablePointable> itemTagTypes1)
            throws HyracksDataException, AsterixException {
        // Probe phase: Probe items from second list
        for(int index1=0; index1<items1.size(); index1++) {
            IVisitablePointable item1 = items1.get(index1);
            byte[] buf = item1.getByteArray();
            int off = item1.getStartOffset();
            int len = item1.getLength();
            keyEntry.set(buf, off, len);
            BinaryEntry entry = hashMap.get(keyEntry);

            // The fieldnames doesn't match
            if (entry == null) {
                return false;
            }

            int index0 = IntegerPointable.getInteger(entry.buf, entry.off);
            ATypeTag fieldType0 = PointableUtils.getTypeTag(itemTagTypes0.get(index0));
            if(fieldType0 != PointableUtils.getTypeTag(itemTagTypes1.get(index1))) {
                return false;
            }

            if (!compareListItems(fieldType0, items0.get(index0), item1))
                return false;
        }
        return true;
    }
}

