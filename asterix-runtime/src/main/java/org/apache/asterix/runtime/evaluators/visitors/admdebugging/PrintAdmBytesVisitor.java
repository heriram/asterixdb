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
import org.apache.asterix.runtime.evaluators.functions.PointableUtils;
import org.apache.asterix.runtime.evaluators.functions.PointableValueDecoder;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PrintAdmBytesVisitor implements
        IVisitablePointableVisitor<Void, Triple<IVisitablePointable, IAType, Long>> {

    private final Map<IVisitablePointable, PrintBytesRecordAccessor> raccessorToPrint =
            new HashMap<IVisitablePointable, PrintBytesRecordAccessor>();
    private final Map<IVisitablePointable, PrintBytesListAccessor> laccessorToPrint =
            new HashMap<IVisitablePointable, PrintBytesListAccessor>();

    private PrintBytesRecordAccessor printBytesRecordAccessor;
    private PrintBytesListAccessor printBytesListAccessor;

    private PointableValueDecoder pvd;
    private PointableUtils pu;

    private long maxLevel = 0;

    public PrintAdmBytesVisitor(PointableValueDecoder pvd, PointableUtils pu, long maxLevel) {
        this.pu = pu;
        this.pvd = pvd;
        this.maxLevel = maxLevel;
    }

    public PrintAdmBytesVisitor() {
        this.pu = new PointableUtils();
        this.pvd = new PointableValueDecoder(pu);
        this.maxLevel = 0;
    }

    @Override public Void visit(AListVisitablePointable accessor, Triple<IVisitablePointable, IAType, Long> arg)
            throws AsterixException {
        printBytesListAccessor = laccessorToPrint.get(accessor);
        if(printBytesListAccessor ==null) {
            printBytesListAccessor = new PrintBytesListAccessor();
            laccessorToPrint.put(accessor, printBytesListAccessor);
        }

        try {
            printBytesListAccessor.accessList(accessor, this,maxLevel, arg);
        } catch (IOException e) {
            throw new AsterixException("Error in trying accessing the list.");
        }
        return null;
    }

    @Override public Void visit(ARecordVisitablePointable accessor, Triple<IVisitablePointable, IAType, Long> arg)
            throws AsterixException {
        ARecordType resultType = (ARecordType) arg.second;
        long nestedLevel = arg.third;

        printBytesRecordAccessor = raccessorToPrint.get(accessor);
        if(printBytesRecordAccessor ==null) {
            printBytesRecordAccessor = new PrintBytesRecordAccessor(pvd, pu, maxLevel);
            raccessorToPrint.put(accessor, printBytesRecordAccessor);
        }

        try {
            ARecordType clonedResultType = null;
            if (resultType != null ) {
                //cloning result type to avoid race conditions during comparison\hash calculation
                clonedResultType = new ARecordType(resultType.getTypeName(), resultType.getFieldNames(),
                        resultType.getFieldTypes(), resultType.isOpen());
            } else {
                clonedResultType = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
            }
            printBytesRecordAccessor.accessRecord(accessor, this, clonedResultType, nestedLevel, arg.first);

        } catch (IOException|AlgebricksException e) {
            new AsterixException("Error in trying accessing the record.");
        }

        return null;
    }

    @Override public Void visit(AFlatValuePointable accessor, Triple<IVisitablePointable, IAType, Long> arg)
            throws AsterixException {
        writeAnnotatedBytes(accessor, arg.second, arg.first);
        return null;
    }

    public void writeAnnotatedBytes(IVisitablePointable accessor, IAType requiredType,
            IVisitablePointable resultAccessor) throws AsterixException {
        try {
            if (requiredType!=null && requiredType.getTypeTag() == ATypeTag.RECORD) {
                pvd.getAnnotatedByteArray(accessor, (ARecordType)requiredType, resultAccessor);
            } else {
                pvd.getAnnotatedByteArray(accessor, resultAccessor);
            }

        } catch (IOException e) {
            throw new AsterixException(e);
        }
    }
}
