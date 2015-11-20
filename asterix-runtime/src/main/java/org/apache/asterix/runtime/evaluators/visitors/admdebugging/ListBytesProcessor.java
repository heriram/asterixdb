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
package org.apache.asterix.runtime.evaluators.visitors.admdebugging;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.runtime.RuntimeRecordTypeInfo;
import org.apache.asterix.runtime.evaluators.functions.PointableUtils;
import org.apache.asterix.runtime.evaluators.functions.AdmToBytesHelper;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;

public class ListBytesProcessor {
    // pointable allocator
    private final PointableAllocator allocator = new PointableAllocator();
    // for storing the cast result
    private final IVisitablePointable itemTempReference = allocator.allocateEmpty();
    private final Triple<IVisitablePointable, Pair<IAType, RuntimeRecordTypeInfo>, Long> itemVisitorArg = new Triple<>(
            itemTempReference, new Pair<>(null, new RuntimeRecordTypeInfo()), 1L);
    private final UnorderedListBuilder unOrderedListBuilder = new UnorderedListBuilder();
    private final OrderedListBuilder orderedListBuilder = new OrderedListBuilder();
    private final ByteArrayAccessibleOutputStream dataBos = new ByteArrayAccessibleOutputStream();
    private final DataOutput dataDos = new DataOutputStream(dataBos);
    private AdmToBytesHelper admToBytesHelper;
    private long outputLevel;
    private IAType reqItemType;

    public ListBytesProcessor(AdmToBytesHelper admToBytesHelper, long outputLevel) {
        this.admToBytesHelper = admToBytesHelper;
        this.outputLevel = outputLevel;
    }

    public void accessList(AListVisitablePointable pointable, AdmToBytesVisitor visitor, IAType requiredType,
            long nestedLevel, IVisitablePointable resultPointable) throws AsterixException {
        // Printing just the highest level of the annotated bytes
        if (outputLevel == 1
                || (nestedLevel == outputLevel && requiredType.getTypeTag() != ATypeTag.UNORDEREDLIST && requiredType
                        .getTypeTag() != ATypeTag.ORDEREDLIST)) {
            if (requiredType != null && requiredType.getTypeTag() == ATypeTag.RECORD) {
                admToBytesHelper.getAnnotatedByteArray(pointable, (ARecordType) requiredType, resultPointable);
            } else {
                admToBytesHelper
                        .getAnnotatedByteArray(pointable, DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, resultPointable);
            }
            return;
        }

        if (requiredType != null && requiredType.getTypeTag() != ATypeTag.ANY) {
            if (requiredType.getTypeTag().equals(ATypeTag.UNORDEREDLIST)) {
                unOrderedListBuilder.reset((AUnorderedListType) requiredType);
                reqItemType = ((AUnorderedListType) requiredType).getItemType();
            }
            if (requiredType.getTypeTag().equals(ATypeTag.ORDEREDLIST)) {
                orderedListBuilder.reset((AOrderedListType) requiredType);
                reqItemType = ((AOrderedListType) requiredType).getItemType();
            }
        } else {
            if (pointable.ordered()) {
                orderedListBuilder.reset(DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE);
            } else {
                orderedListBuilder.reset(DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE);
            }
            reqItemType = BuiltinType.ANY;
        }

        List<IVisitablePointable> items = pointable.getItems();
        List<IVisitablePointable> itemTypeTags = pointable.getItemTags();


        try {
            for (int i = 0; i < items.size(); i++) {
                IVisitablePointable item = items.get(i);
                IVisitablePointable itemTypeTag = itemTypeTags.get(i);
                ATypeTag typeTag = PointableUtils.getTypeTag(itemTypeTag);

                // If reached the max nesting level just "print" the annotated byte record for this item
                if (nestedLevel == outputLevel) {
                    if (reqItemType != null && reqItemType.getTypeTag() == ATypeTag.RECORD) {
                        admToBytesHelper.getAnnotatedByteArray(item, (ARecordType) reqItemType, itemTempReference);
                    } else {
                        admToBytesHelper.getAnnotatedByteArray(item, DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE,
                                itemTempReference);
                    }
                } else {
                    if (reqItemType == null || reqItemType.getTypeTag().equals(ATypeTag.ANY)) {
                        itemVisitorArg.second.first = DefaultOpenFieldType.getDefaultOpenFieldType(typeTag);
                    } else {
                        itemVisitorArg.second.first = reqItemType;
                        if (reqItemType.getTypeTag().equals(ATypeTag.RECORD)) {
                            itemVisitorArg.second.second.reset((ARecordType) reqItemType);
                        }
                    }
                    itemVisitorArg.third = nestedLevel + 1;
                    item.accept(visitor, itemVisitorArg);
                }
                if (requiredType.getTypeTag().equals(ATypeTag.ORDEREDLIST)) {
                    orderedListBuilder.addItem(itemTempReference);
                }
                if (requiredType.getTypeTag().equals(ATypeTag.UNORDEREDLIST)) {
                    unOrderedListBuilder.addItem(itemTempReference);
                }

            }
            dataBos.reset();
            if (requiredType.getTypeTag().equals(ATypeTag.ORDEREDLIST)) {
                orderedListBuilder.write(dataDos, true);
            }
            if (requiredType.getTypeTag().equals(ATypeTag.UNORDEREDLIST)) {
                unOrderedListBuilder.write(dataDos, true);
            }
            resultPointable.set(dataBos.getByteArray(), 0, dataBos.size());
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
    }
}
