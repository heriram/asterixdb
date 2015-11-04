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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

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

    private PrintAdmBytesHelper printHelper = PrintAdmBytesHelper.getInstance();

    private long maxLevel = 0;

    @Override public Void visit(AListVisitablePointable accessor, Triple<IVisitablePointable, IAType, Long> arg)
            throws AsterixException {
        printBytesListAccessor = laccessorToPrint.get(accessor);
        if(printBytesListAccessor ==null) {
            printBytesListAccessor = new PrintBytesListAccessor();
            laccessorToPrint.put(accessor, printBytesListAccessor);
        }
        try {
            if (arg.third < maxLevel && arg.second!=null && arg.second.getTypeTag()!= ATypeTag.RECORD) {
                AbstractCollectionType resultType = (AbstractCollectionType) arg.second;
                if(arg.second.getTypeTag()==ATypeTag.ANY) {
                    arg.second = accessor.ordered()?
                            DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE:
                            DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE;
                }
                //cloning result type to avoid race conditions during comparison\hash calculation
                AbstractCollectionType clonedResultType = accessor.ordered()?
                        new AOrderedListType(resultType.getItemType(), resultType.getTypeName()):
                        new AUnorderedListType(resultType.getItemType(), resultType.getTypeName());

                printBytesListAccessor.accessList(accessor, this, maxLevel, arg);
            } else {
                printAnnotatedBytes(accessor, arg.first);
            }
        } catch (IOException e) {
            new AsterixException("Error in trying accessing the record.");
        }
        return null;
    }

    @Override public Void visit(ARecordVisitablePointable accessor, Triple<IVisitablePointable, IAType, Long> arg)
            throws AsterixException {
        printBytesRecordAccessor = raccessorToPrint.get(accessor);
        if(printBytesRecordAccessor ==null) {
            printBytesRecordAccessor = new PrintBytesRecordAccessor();
            raccessorToPrint.put(accessor, printBytesRecordAccessor);
        }
        try {
            ARecordType resultType = (ARecordType) arg.second;
            if (resultType == null ) {
                resultType = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
            }
                //cloning result type to avoid race conditions during comparison\hash calculation
            ARecordType clonedResultType = new ARecordType(resultType.getTypeName(), resultType.getFieldNames(),
                        resultType.getFieldTypes(), resultType.isOpen());

            printBytesRecordAccessor.accessRecord(accessor, this, maxLevel, clonedResultType, arg.third, arg.first);

        } catch (IOException|AlgebricksException e) {
            new AsterixException("Error in trying accessing the record.");
        }

        return null;
    }

    @Override public Void visit(AFlatValuePointable accessor, Triple<IVisitablePointable, IAType, Long> arg)
            throws AsterixException {
        try {
            printAnnotatedBytes(accessor, arg.first);
        } catch (IOException e) {
            throw new AsterixException("Error printing annotated byte array from a flat object");
        }
        return null;
    }


    private void printAnnotatedBytes(IVisitablePointable accessor, IVisitablePointable resultAccessor)
            throws IOException, AsterixException {
        ArrayBackedValueStorage tabvs = printHelper.getTempBuffer();
        tabvs.reset();
        printHelper.printAnnotatedBytes(accessor, tabvs.getDataOutput());

        resultAccessor.set(tabvs);
    }

    public void setMaxLevel(long maxLevel) {
        this.maxLevel = maxLevel;
    }

    public PrintAdmBytesHelper getPrintHelper() {
        return printHelper;
    }

    public void resetPrintHelper() {
        this.printHelper.reset();
    }

}
