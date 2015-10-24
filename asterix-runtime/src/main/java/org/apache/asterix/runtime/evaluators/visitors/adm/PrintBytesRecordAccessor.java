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

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.functions.PointableUtils;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

import java.io.IOException;
import java.util.List;

class PrintBytesRecordAccessor {
    private Triple<ATypeTag, Object, Long> arg;
    private PrintAdmBytesHelper printHelper;

    public PrintBytesRecordAccessor() {
    }

    public IValueReference accessRecord(ARecordVisitablePointable accessor, PrintAdmBytesVisitor visitor, long maxLevel,
            Triple<ATypeTag, Object, Long> arg)
            throws AsterixException, IOException {

        this.arg = arg;
        printHelper = visitor.getPrintHelper();


        List<IVisitablePointable> fieldNames = accessor.getFieldNames();
        List<IVisitablePointable> fieldValues = accessor.getFieldValues();
        List<IVisitablePointable> fieldTypeTags = accessor.getFieldTypeTags();

        RecordBuilder recordBuilder = (RecordBuilder)arg.second;
        if(PointableUtils.isAList(arg.first)) {
            recordBuilder = new RecordBuilder();
            ARecordType recordType = accessor.getInputRecordType();
            if (recordType==null)
                recordType = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
            recordBuilder.reset(recordType);
        }


        for (int i = 0; i < fieldNames.size(); i++) {
            IValueReference printedValue = printFieldValue(visitor, fieldNames, fieldValues, fieldTypeTags, i, maxLevel);
            String fieldName = PointableUtils.INSTANCE.getFieldName(fieldNames.get(i));
            int pos = recordBuilder.getFieldId(fieldName);
            if (pos>-1) {
                recordBuilder.addField(pos, printedValue);
            } else {
                recordBuilder.addField(fieldNames.get(i), printedValue);
            }
        }

        if(PointableUtils.isAList(arg.first)) {
            ArrayBackedValueStorage tempBuffer = printHelper.getTempBuffer();
            tempBuffer.reset();
            recordBuilder.write(tempBuffer.getDataOutput(), true);
            return tempBuffer;
        } else {
            // No return needed  (arg.second)
            return null;
        }


    }

    private IValueReference printFieldValue(PrintAdmBytesVisitor visitor, List<IVisitablePointable> fieldNames,
            List<IVisitablePointable> fieldValues, List<IVisitablePointable> fieldTags, int i, long maxLevel)
            throws AsterixException, IOException {

        IVisitablePointable value = fieldValues.get(i);
        ATypeTag typeTag = PointableUtils.getTypeTag(fieldTags.get(i));
        IValueReference printedValue = null;

        switch (typeTag) {
            case RECORD:
                if (arg.third <= maxLevel) {
                    printedValue = ((ARecordVisitablePointable) value).accept(visitor, arg);
                    arg.third++; // Increase the current level
                } else {
                    printedValue = printHelper.getTempBuffer();
                    ((ArrayBackedValueStorage) printedValue).reset();
                    printHelper.printAnnotatedBytes(value, ((ArrayBackedValueStorage) printedValue).getDataOutput());
                }
                break;
            case ORDEREDLIST:
            case UNORDEREDLIST:
                if (arg.third <= maxLevel) {
                    printedValue = ((AListVisitablePointable) value).accept(visitor, arg);
                    arg.third++; // Increase the current level
                } else {
                    printedValue = printHelper.getTempBuffer();
                    ((ArrayBackedValueStorage) printedValue).reset();
                    printHelper.printAnnotatedBytes(value, ((ArrayBackedValueStorage) printedValue).getDataOutput());
                }
                break;
            default:
                printedValue = ((AFlatValuePointable) value).accept(visitor, arg);
        }

        return printedValue;
    }

}
