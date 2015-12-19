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

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.runtime.RuntimeRecordTypeInfo;
import org.apache.asterix.runtime.evaluators.functions.AdmToBytesHelper;
import org.apache.hyracks.algebricks.common.utils.Triple;

public class AdmToBytesVisitor implements
        IVisitablePointableVisitor<IVisitablePointable, Triple<IAType, RuntimeRecordTypeInfo, Long>> {

    private final Map<IVisitablePointable, RecordBytesProcessor> rProcessorToAnnotatedBytes = new HashMap<>();
    private final Map<IVisitablePointable, ListBytesProcessor> lProcessorToAnnotatedBytes = new HashMap<>();
    private final PointableAllocator allocator = new PointableAllocator();
    private final IVisitablePointable resultPointable = allocator.allocateEmpty();
    private RecordBytesProcessor recordBytesProcessor;
    private ListBytesProcessor listBytesProcessor;
    private AdmToBytesHelper admToBytesHelper;

    private long outputLevel = 0;

    public AdmToBytesVisitor(AdmToBytesHelper admToBytesHelper) {
        this.admToBytesHelper = admToBytesHelper;
    }

    public void setOutputLevel(long outputLevel) {
        this.outputLevel = outputLevel;
    }

    @Override
    public IVisitablePointable visit(AListVisitablePointable pointable, Triple<IAType, RuntimeRecordTypeInfo, Long> arg)
            throws AsterixException {
        if (outputLevel == 1
                || (arg.third == outputLevel && arg.first.getTypeTag() != ATypeTag.UNORDEREDLIST && arg.first
                        .getTypeTag() != ATypeTag.ORDEREDLIST)) {
            computeResultPointable(pointable, arg.first, resultPointable);
            return resultPointable;
        }

        listBytesProcessor = lProcessorToAnnotatedBytes.get(pointable);
        if (listBytesProcessor == null) {
            listBytesProcessor = new ListBytesProcessor();
            lProcessorToAnnotatedBytes.put(pointable, listBytesProcessor);
        }
        if (arg.first.getTypeTag() == ATypeTag.ANY) {
            arg.first = DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE;
            if (pointable.ordered()) {
                arg.first = DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE;
            }
        }
        listBytesProcessor.accessList(pointable, outputLevel, this, arg.first, arg.third, resultPointable);
        return resultPointable;
    }

    @Override
    public IVisitablePointable visit(ARecordVisitablePointable pointable,
            Triple<IAType, RuntimeRecordTypeInfo, Long> arg) throws AsterixException {

        if (outputLevel == 1 || arg.third == outputLevel) {
            computeResultPointable(pointable, arg.first, resultPointable);
            return resultPointable;
        }

        RuntimeRecordTypeInfo runtimeRecordTypeInfo = arg.second;
        if (arg.first.getTypeTag() == ATypeTag.ANY) {
            arg.first = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
        }
        ARecordType requiredType = (ARecordType) arg.first;

        recordBytesProcessor = rProcessorToAnnotatedBytes.get(pointable);
        if (recordBytesProcessor == null) {
            recordBytesProcessor = new RecordBytesProcessor();
            rProcessorToAnnotatedBytes.put(pointable, recordBytesProcessor);
        }
        recordBytesProcessor.accessRecord(pointable, this, requiredType, runtimeRecordTypeInfo, arg.third,
                resultPointable);
        return resultPointable;
    }

    @Override
    public IVisitablePointable visit(AFlatValuePointable pointable, Triple<IAType, RuntimeRecordTypeInfo, Long> arg)
            throws AsterixException {
        computeResultPointable(pointable, arg.first, resultPointable);
        return resultPointable;
    }

    public void computeResultPointable(IVisitablePointable inputPointable, IAType requiredType,
            IVisitablePointable outputPointable) throws AsterixException {
        if (requiredType != null && requiredType.getTypeTag() == ATypeTag.RECORD) {
            admToBytesHelper.getAnnotatedByteArray(inputPointable, (ARecordType) requiredType, outputPointable);
        } else {
            admToBytesHelper.getAnnotatedByteArray(inputPointable, DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE,
                    outputPointable);
        }
    }
}
