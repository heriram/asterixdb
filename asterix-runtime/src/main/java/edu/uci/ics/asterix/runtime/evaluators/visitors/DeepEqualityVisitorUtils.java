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

import edu.uci.ics.asterix.dataflow.data.nontagged.comparators.ListItemBinaryComparatorFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.hash.ListItemBinaryHashFunctionFactory;
import edu.uci.ics.asterix.runtime.evaluators.functions.BinaryHashMap;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;

import java.util.Arrays;

public class DeepEqualityVisitorUtils {

    // Default values
    public static final int TABLE_SIZE = 100;
    public static final int TABLE_FRAME_SIZE = 32768;

    private static IBinaryHashFunction putHashFunc = ListItemBinaryHashFunctionFactory.INSTANCE.createBinaryHashFunction();
    private static IBinaryHashFunction getHashFunc = ListItemBinaryHashFunctionFactory.INSTANCE.createBinaryHashFunction();
    private static IBinaryComparator cmp = ListItemBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    private static BinaryHashMap hashMap = null;

    public static BinaryHashMap initializeHashMap(BinaryHashMap.BinaryEntry valEntry) {
        return initializeHashMap(0, 0, valEntry);
    }

    public static BinaryHashMap initializeHashMap(int tableSize, int tableFrameSize, BinaryHashMap.BinaryEntry valEntry) {
        if (tableFrameSize != 0 && tableSize != 0) {
            hashMap = new BinaryHashMap(tableSize, tableFrameSize, putHashFunc, getHashFunc, cmp);
        } else {
            hashMap = new BinaryHashMap(TABLE_SIZE, TABLE_FRAME_SIZE, putHashFunc, getHashFunc, cmp);
        }

        byte[] emptyValBuf = new byte[8];
        Arrays.fill(emptyValBuf, (byte) 0);
        valEntry.set(emptyValBuf, 0, 8);
        return hashMap;
    }

}
