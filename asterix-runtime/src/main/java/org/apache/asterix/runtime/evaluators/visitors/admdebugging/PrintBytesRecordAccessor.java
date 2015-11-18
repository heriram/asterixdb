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

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.functions.PointableUtils;
import org.apache.asterix.runtime.evaluators.functions.PointableValueDecoder;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
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
    private final IVisitablePointable tempFieldReference = allocator.allocateEmpty();
    private final Triple<IVisitablePointable, IAType, Long> nestedVisitorArg =
            new Triple<IVisitablePointable, IAType, Long>(tempFieldReference, null, 0L);

    PointableValueDecoder pvd;
    PointableUtils pu;

    public PrintBytesRecordAccessor(PointableValueDecoder pvd, PointableUtils pu, long maxLevel) {
        this.maxLevel = maxLevel;
        this.pvd = pvd;
        this.pu = pu;
    }

    public void accessRecord(ARecordVisitablePointable accessor, PrintAdmBytesVisitor visitor,
            ARecordType requiredType, long nestedLevel, IVisitablePointable resultAccessor)
            throws AsterixException, IOException, AlgebricksException {

        if (maxLevel==1 || nestedLevel == maxLevel) {
            // No need to go further than printing out the annotated bytes
            visitor.writeAnnotatedBytes(accessor, requiredType, resultAccessor);

        } else {

            outputBos.reset();

            List<IVisitablePointable> fieldNames = accessor.getFieldNames();
            List<IVisitablePointable> fieldValues = accessor.getFieldValues();

            IARecordBuilder recordBuilder = pu.getRecordBuilder();
            recordBuilder.reset(requiredType);
            recordBuilder.init();

            for (int i = 0; i < fieldNames.size(); i++) {
                IVisitablePointable fieldValue = fieldValues.get(i);
                IVisitablePointable fieldName = fieldNames.get(i);
                String fname = pu.getFieldName(fieldName);

                int pos = recordBuilder.getFieldId(fname);
                nestedVisitorArg.second = requiredType.getFieldType(fname);
                nestedVisitorArg.third = nestedLevel + 1;
                fieldValue.accept(visitor, nestedVisitorArg);
                tempFieldReference.set(nestedVisitorArg.first);

                if (pos > -1) {
                    recordBuilder.addField(pos, tempFieldReference);
                } else {
                    recordBuilder.addField(fieldNames.get(i), tempFieldReference);
                }

            }
            recordBuilder.write(outputDos, true);
            resultAccessor.set(outputBos.getByteArray(), 0, outputBos.size());
        }
    }


}


