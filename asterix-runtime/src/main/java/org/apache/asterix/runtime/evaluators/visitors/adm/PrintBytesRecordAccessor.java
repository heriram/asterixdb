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
import org.apache.asterix.dataflow.data.nontagged.printers.adm.ARecordPrinterFactory;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.functions.PointableUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

class PrintBytesRecordAccessor {
    private final ByteArrayAccessibleOutputStream outputBos = new ByteArrayAccessibleOutputStream();
    private final DataOutputStream outputDos = new DataOutputStream(outputBos);
    private final PrintStream ps = new PrintStream(outputBos);

    private long maxLevel;

    // pointable allocator
    private final PointableAllocator allocator = new PointableAllocator();
    private final IVisitablePointable fieldTempReference = allocator.allocateEmpty();
    private final Triple<IVisitablePointable, IAType, Long> nestedVisitorArg =
            new Triple<IVisitablePointable, IAType, Long>(fieldTempReference, null, 0L);

    private PrintAdmBytesHelper printHelper;

    public PrintBytesRecordAccessor() {
    }

    public void accessRecord(ARecordVisitablePointable accessor, PrintAdmBytesVisitor visitor, long maxLevel,
            ARecordType requiredType, long level, IVisitablePointable resultAccessor)
            throws AsterixException, IOException, AlgebricksException {
        printHelper = visitor.getPrintHelper();
        this.maxLevel = maxLevel;
        outputBos.reset();

        ARecordPrinterFactory recordPrinter = new ARecordPrinterFactory(requiredType);
        IPrinter rp = recordPrinter.createPrinter();

        if (level<=1 || level==maxLevel) {
            ArrayBackedValueStorage tabvs = new ArrayBackedValueStorage();
            tabvs.reset();

            printHelper.printAnnotatedBytes(accessor, tabvs.getDataOutput());
            rp.print(tabvs.getByteArray(),tabvs.getStartOffset(), tabvs.getLength(), ps);
            String recprint = outputBos.toString("UTF8");
        } else {

            List<IVisitablePointable> fieldNames = accessor.getFieldNames();
            List<IVisitablePointable> fieldTypes = accessor.getFieldTypeTags();
            List<IVisitablePointable> fieldValues = accessor.getFieldValues();

            RecordBuilder recordBuilder = new RecordBuilder(); //printHelper.getRecordBuilder();
            recordBuilder.init();
            recordBuilder.reset(requiredType);

            for (int i = 0; i < fieldNames.size(); i++) {
                IVisitablePointable fieldValue = fieldValues.get(i);
                IVisitablePointable fieldName = fieldNames.get(i);
                String fname = PointableUtils.INSTANCE.getFieldName(fieldName);

                if (PointableUtils.isType(ATypeTag.RECORD, fieldTypes.get(i))) {
                    nestedVisitorArg.second = printHelper
                            .loadRequireType(((ARecordVisitablePointable) fieldValue).getInputRecordType(), level + 1,
                                    maxLevel);
                }
                nestedVisitorArg.third = level + 1;
                fieldValue.accept(visitor, nestedVisitorArg);
                fieldTempReference.set(nestedVisitorArg.first);

                int pos = recordBuilder.getFieldId(fname);
                if (pos > -1) {
                    recordBuilder.addField(pos, fieldTempReference);
                } else {
                    recordBuilder.addField(fieldNames.get(i), fieldTempReference);
                }
            }
            //ArrayBackedValueStorage tempBuffer = printHelper.getTempBuffer();
            //tempBuffer.reset();
            recordBuilder.write(outputDos, true);
            rp.print(outputBos.getByteArray(), 0, outputBos.size(), ps);
            String recprint = outputBos.toString("UTF8");
        }

        resultAccessor.set(outputBos.getByteArray(), 0, outputBos.size());
    }

}
