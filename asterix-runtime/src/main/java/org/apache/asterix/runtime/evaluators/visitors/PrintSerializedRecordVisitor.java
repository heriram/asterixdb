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
import org.apache.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import org.apache.hyracks.algebricks.common.utils.Triple;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PrintSerializedRecordVisitor implements
        IVisitablePointableVisitor<Void, Triple<IVisitablePointable, StringBuilder, Long>> {

    private Triple<IVisitablePointable, StringBuilder, Long> arg;

    private final Map<IVisitablePointable, RecordPrintBytesAccessor> raccessorToPrint =
            new HashMap<IVisitablePointable, RecordPrintBytesAccessor>();

    private RecordPrintBytesAccessor recordPrintBytesAccessor;

    private long maxLevel = 0;

    @Override public Void visit(AListVisitablePointable accessor, Triple<IVisitablePointable, StringBuilder, Long> arg)
            throws AsterixException {
        visitNonRecord(accessor, arg);
        return null;
    }

    @Override public Void visit(ARecordVisitablePointable accessor, Triple<IVisitablePointable, StringBuilder, Long> arg)
            throws AsterixException {
        this.arg = arg;
        recordPrintBytesAccessor = raccessorToPrint.get(accessor);
        if(recordPrintBytesAccessor==null) {
            recordPrintBytesAccessor = new RecordPrintBytesAccessor();
            raccessorToPrint.put(accessor, recordPrintBytesAccessor);
        }

        try {
            recordPrintBytesAccessor.accessRecord(accessor, this, maxLevel, arg);
        } catch (IOException e) {
            new AsterixException("Error in trying accessing the record.");
        }

        return null;
    }

    @Override public Void visit(AFlatValuePointable accessor, Triple<IVisitablePointable, StringBuilder, Long> arg)
            throws AsterixException {
        visitNonRecord(accessor, arg);
        return null;
    }

    private void visitNonRecord(IVisitablePointable accessor, Triple<IVisitablePointable, StringBuilder, Long> arg)
            throws AsterixException {
        this.arg = arg;
        try {
            arg.second.append("\"" + recordPrintBytesAccessor.getFieldName(arg.first) + "\": ");
        } catch (IOException e) {
            throw new AsterixException("Unable to get the field name");
        }
        arg.second.append(recordPrintBytesAccessor.printAnnotatedBytes(accessor));
    }

    public void setMaxLevel(long maxLevel) {
        this.maxLevel = maxLevel;
    }

}
