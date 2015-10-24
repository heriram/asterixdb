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
package org.apache.asterix.runtime.evaluators.visitors.adm;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.runtime.evaluators.functions.PointableUtils;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

import java.io.IOException;
import java.util.List;

public class PrintBytesListAccessor {

    private Triple<ATypeTag, Object, Long> arg;
    private PrintAdmBytesHelper printHelper;


    public IValueReference accessList(AListVisitablePointable accessor, PrintAdmBytesVisitor visitor, long maxLevel,
            Triple<ATypeTag, Object, Long> arg)
            throws AsterixException, IOException {
        this.arg = arg;
        this.printHelper = visitor.getPrintHelper();


        List<IVisitablePointable> items =  accessor.getItems();
        List<IVisitablePointable> itemTags =  accessor.getItemTags();


        OrderedListBuilder listBuilder = (OrderedListBuilder)arg.second;
        if(arg.first==ATypeTag.RECORD) {
            listBuilder = new OrderedListBuilder();
            AbstractCollectionType listType = (PointableUtils.getTypeTag(accessor)==ATypeTag.ORDEREDLIST)?
                    DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE:
                    DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE;
            listBuilder.reset(listType);
        }

        // If we are printing from a list as the complex object
        for (int i = 0; i < items.size(); i++) {
            IValueReference printedValue = printItemValue(visitor, items, itemTags, i, maxLevel);
            listBuilder.addItem(printedValue);
        }

        if(arg.first==ATypeTag.RECORD) {
            ArrayBackedValueStorage tempBuffer = printHelper.getTempBuffer();
            tempBuffer.reset();
            listBuilder.write(tempBuffer.getDataOutput(), true);
            return tempBuffer;
        } else {
            return null; // No need to return anything
        }


    }

    private IValueReference printItemValue(PrintAdmBytesVisitor visitor, List<IVisitablePointable> items,
            List<IVisitablePointable> itemTags, int i, long maxLevel)
            throws AsterixException, IOException {
        IValueReference printedValue = null;

        IVisitablePointable itemType = itemTags.get(i);
        IVisitablePointable item = items.get(i);

        ATypeTag typeTag = PointableUtils.getTypeTag(itemType);
        switch (typeTag) {
            case RECORD:
                if (arg.third <= maxLevel) {
                    printedValue = ((ARecordVisitablePointable) item).accept(visitor, arg);
                    arg.third++; // Increase the current level
                } else {
                    printedValue = printHelper.getTempBuffer();
                    ((ArrayBackedValueStorage) printedValue).reset();
                    printHelper.printAnnotatedBytes(item, ((ArrayBackedValueStorage) printedValue).getDataOutput());
                }
                break;
            case ORDEREDLIST:
            case UNORDEREDLIST:
                if (arg.third <= maxLevel) {
                    printedValue = ((AListVisitablePointable) item).accept(visitor, arg);
                    arg.third++; // Increase the current level
                } else {
                    printedValue = printHelper.getTempBuffer();
                    ((ArrayBackedValueStorage) printedValue).reset();
                    printHelper.printAnnotatedBytes(item, ((ArrayBackedValueStorage) printedValue).getDataOutput());
                }
                break;
            default:
                printedValue = ((AFlatValuePointable) item).accept(visitor, arg);
        }

        return printedValue;
    }

}
