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
package org.apache.asterix.builders;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class PointableTypeVisitor  implements IVisitablePointableVisitor<Void, Pair<Boolean, Void>> {

    @Override public Void visit(AListVisitablePointable accessor, Pair<Boolean, Void> arg) throws AsterixException {
        arg.first = false;
        return null;
    }

    @Override public Void visit(ARecordVisitablePointable accessor, Pair<Boolean, Void> arg)
            throws AsterixException {
        ARecordType recType = ((ARecordVisitablePointable)accessor).getInputRecordType();
        arg.first =  recType.getFieldNames().length>0;
        return null;
    }

    @Override public Void visit(AFlatValuePointable accessor, Pair<Boolean, Void> arg) throws AsterixException {
        arg.first = false;
        return null;
    }
}
