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

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PrintAdmBytesVisitor implements
        IVisitablePointableVisitor<Void, Triple<IVisitablePointable, RecordBuilder, Long>> {

    private final Map<IVisitablePointable, PrintAdmBytesAccessor> raccessorToPrint =
            new HashMap<IVisitablePointable, PrintAdmBytesAccessor>();

    private PrintAdmBytesAccessor printAdmBytesAccessor;

    private long maxLevel = 0;

    @Override public Void visit(AListVisitablePointable accessor, Triple<IVisitablePointable, RecordBuilder, Long> arg)
            throws AsterixException {
        visitNonRecord(accessor, arg);
        return null;
    }

    @Override public Void visit(ARecordVisitablePointable accessor, Triple<IVisitablePointable, RecordBuilder, Long> arg)
            throws AsterixException {
        printAdmBytesAccessor = raccessorToPrint.get(accessor);
        if(printAdmBytesAccessor ==null) {
            printAdmBytesAccessor = new PrintAdmBytesAccessor();
            raccessorToPrint.put(accessor, printAdmBytesAccessor);
        }

        try {
            printAdmBytesAccessor.accessRecord(accessor, this, maxLevel, arg);
        } catch (IOException e) {
            new AsterixException("Error in trying accessing the record.");
        }

        return null;
    }

    @Override public Void visit(AFlatValuePointable accessor, Triple<IVisitablePointable, RecordBuilder, Long> arg)
            throws AsterixException {
        visitNonRecord(accessor, arg);
        return null;
    }

    private void visitNonRecord(IVisitablePointable accessor, Triple<IVisitablePointable, RecordBuilder, Long> arg)
            throws AsterixException {
        PrintAdmBytesHelper printHelper = printAdmBytesAccessor.getPrintHelper();
        ArrayBackedValueStorage tabvs = printHelper.getTempBuffer();
        if (arg.first!=null) {
            try {
                printAdmBytesAccessor.printAnnotatedBytes(accessor, tabvs.getDataOutput());
                arg.second.addField(arg.first, tabvs);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void setMaxLevel(long maxLevel) {
        this.maxLevel = maxLevel;
    }

}
