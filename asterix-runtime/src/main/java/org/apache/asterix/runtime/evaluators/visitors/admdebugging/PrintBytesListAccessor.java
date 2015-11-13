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
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;

import java.io.DataOutputStream;
import java.io.IOException;

public class PrintBytesListAccessor {

    private final ByteArrayAccessibleOutputStream outputBos = new ByteArrayAccessibleOutputStream();
    private final DataOutputStream outputDos = new DataOutputStream(outputBos);

    private AbstractCollectionType requiredType = null;

    // pointable allocator
    private final PointableAllocator allocator = new PointableAllocator();
    private final IVisitablePointable fieldTempReference = allocator.allocateEmpty();
    private final Triple<IVisitablePointable, IAType, Long> nestedVisitorArg =
            new Triple<IVisitablePointable, IAType, Long>(fieldTempReference, null, null);

    private PrintAdmBytesHelper printHelper;
    private final ArrayBackedValueStorage tempBuffer = printHelper.getTempBuffer();


    //(accessor, this, maxLevel, clonedResultType, arg.third, arg.first)
    public void accessList(AListVisitablePointable accessor, PrintAdmBytesVisitor visitor, long maxLevel,
             Triple<IVisitablePointable, IAType, Long> arg)
            throws AsterixException, IOException {

        if (maxLevel==1 || arg.third == (maxLevel-2)) {
            // No need to go further than printing out the annotated bytes
            visitor.writeAnnotatedBytes(accessor, arg.second, arg.first);

        } else {
         // TODO Add a way to deal with nested list
            visitor.writeAnnotatedBytes(accessor, arg.second, arg.first);
        }
    }
}
