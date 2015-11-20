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
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.runtime.RuntimeRecordTypeInfo;
import org.apache.asterix.runtime.evaluators.functions.AdmToBytesHelper;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;

public class AdmToBytesVisitor implements
        IVisitablePointableVisitor<Void, Triple<IVisitablePointable, Pair<IAType, RuntimeRecordTypeInfo>, Long>> {

    private final Map<IVisitablePointable, RecordBytesProcessor> rProcessorToAnnotatedBytes = new HashMap<>();
    private final Map<IVisitablePointable, ListBytesProcessor> lProcessorToAnnotatedBytes = new HashMap<>();

    private RecordBytesProcessor recordBytesProcessor;
    private ListBytesProcessor listBytesProcessor;

    private AdmToBytesHelper pvd;

    private long outputLevel = 0;

    public AdmToBytesVisitor(AdmToBytesHelper pvd, long outputLevel) {
        this.pvd = pvd;
        this.outputLevel = outputLevel;
    }

    @Override
    public Void visit(AListVisitablePointable pointable,
            Triple<IVisitablePointable, Pair<IAType, RuntimeRecordTypeInfo>, Long> arg) throws AsterixException {
        listBytesProcessor = lProcessorToAnnotatedBytes.get(pointable);
        if (listBytesProcessor == null) {
            listBytesProcessor = new ListBytesProcessor(pvd, outputLevel);
            lProcessorToAnnotatedBytes.put(pointable, listBytesProcessor);
        }
        if (arg.second.first.getTypeTag() == ATypeTag.ANY) {
            arg.second.first = DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE;
            if (pointable.ordered()) {
                arg.second.first = DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE;
            }
        }
        listBytesProcessor.accessList(pointable, this, arg.second.first, arg.third, arg.first);
        return null;
    }

    @Override
    public Void visit(ARecordVisitablePointable pointable,
            Triple<IVisitablePointable, Pair<IAType, RuntimeRecordTypeInfo>, Long> arg) throws AsterixException {
        RuntimeRecordTypeInfo runtimeRecordTypeInfo = arg.second.second;
        if (arg.second.first.getTypeTag() == ATypeTag.ANY) {
            arg.second.first = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
        }
        ARecordType requiredType = (ARecordType) arg.second.first;

        recordBytesProcessor = rProcessorToAnnotatedBytes.get(pointable);
        if (recordBytesProcessor == null) {
            recordBytesProcessor = new RecordBytesProcessor(pvd, outputLevel);
            rProcessorToAnnotatedBytes.put(pointable, recordBytesProcessor);
        }
        recordBytesProcessor.accessRecord(pointable, this, requiredType, runtimeRecordTypeInfo, arg.third, arg.first);
        return null;
    }

    @Override
    public Void visit(AFlatValuePointable pointable,
            Triple<IVisitablePointable, Pair<IAType, RuntimeRecordTypeInfo>, Long> arg) throws AsterixException {
        IAType requiredType = arg.second.first;
        if (requiredType != null && requiredType.getTypeTag() == ATypeTag.RECORD) {
            pvd.getAnnotatedByteArray(pointable, (ARecordType) requiredType, arg.first);
        } else {
            pvd.getAnnotatedByteArray(pointable, DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, arg.first);
        }
        return null;
    }
}
