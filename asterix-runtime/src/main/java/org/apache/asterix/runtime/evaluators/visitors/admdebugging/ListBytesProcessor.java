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
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;

public class ListBytesProcessor {
    // pointable allocator
    private final PointableAllocator allocator = new PointableAllocator();
    private final IVisitablePointable itemTempReference = allocator.allocateEmpty();
    private final Triple<IAType, RuntimeRecordTypeInfo, Long> itemVisitorArg = new Triple<>(null,
            new RuntimeRecordTypeInfo(), 1L);
    private final UnorderedListBuilder unOrderedListBuilder = new UnorderedListBuilder();
    private final OrderedListBuilder orderedListBuilder = new OrderedListBuilder();
    private final ByteArrayAccessibleOutputStream dataBos = new ByteArrayAccessibleOutputStream();
    private final DataOutput dataDos = new DataOutputStream(dataBos);
    private IAType reqItemType;

    public void accessList(AListVisitablePointable pointable, long outputLevel, AdmToBytesVisitor visitor,
            IAType requiredType, long nestedLevel, IVisitablePointable resultPointable) throws AsterixException {
        // Printing just the highest level of the annotated bytes
        ATypeTag reqTypeTag = requiredType.getTypeTag();

        if (requiredType != null && reqTypeTag != ATypeTag.ANY) {
            if (reqTypeTag == ATypeTag.UNORDEREDLIST) {
                unOrderedListBuilder.reset((AUnorderedListType) requiredType);
                reqItemType = ((AUnorderedListType) requiredType).getItemType();
            }
            if (reqTypeTag == ATypeTag.ORDEREDLIST) {
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
                    visitor.computeResultPointable(item, reqItemType, itemTempReference);
                    addListItem(reqTypeTag, itemTempReference);
                } else {
                    if (reqItemType == null || reqItemType.getTypeTag().equals(ATypeTag.ANY)) {
                        itemVisitorArg.first = DefaultOpenFieldType.getDefaultOpenFieldType(typeTag);
                    } else {
                        itemVisitorArg.first = reqItemType;
                        if (reqItemType.getTypeTag().equals(ATypeTag.RECORD)) {
                            itemVisitorArg.second.reset((ARecordType) reqItemType);
                        }
                    }
                    itemVisitorArg.third = nestedLevel + 1;
                    addListItem(reqTypeTag, (item.accept(visitor, itemVisitorArg)));
                }
            }
            dataBos.reset();
            if (reqTypeTag == ATypeTag.ORDEREDLIST) {
                orderedListBuilder.write(dataDos, true);
            }
            if (reqTypeTag == ATypeTag.UNORDEREDLIST) {
                unOrderedListBuilder.write(dataDos, true);
            }
            resultPointable.set(dataBos.getByteArray(), 0, dataBos.size());
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
    }

    private void addListItem(ATypeTag listTypeTag, IValueReference itemReference) throws HyracksDataException {
        if (listTypeTag == ATypeTag.ORDEREDLIST) {
            orderedListBuilder.addItem(itemReference);
        }
        if (listTypeTag == ATypeTag.UNORDEREDLIST) {
            unOrderedListBuilder.addItem(itemReference);
        }
    }
}
