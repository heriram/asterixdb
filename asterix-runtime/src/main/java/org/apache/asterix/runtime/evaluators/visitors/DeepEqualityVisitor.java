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
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy.Domain;
import org.apache.asterix.runtime.evaluators.functions.PointableUtils;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import java.util.HashMap;
import java.util.Map;

public class DeepEqualityVisitor implements IVisitablePointableVisitor<Void, Pair<IVisitablePointable, Boolean>> {
    private final Map<IVisitablePointable, ListDeepEqualityAccessor> laccessorToEqulity =
            new HashMap<IVisitablePointable, ListDeepEqualityAccessor>();
    private final Map<IVisitablePointable, RecordDeepEqualityAccessor> raccessorToEquality =
            new HashMap<IVisitablePointable, RecordDeepEqualityAccessor>();

    @Override public Void visit(AListVisitablePointable accessor, Pair<IVisitablePointable, Boolean> arg)
            throws AsterixException {
        ListDeepEqualityAccessor listDeepEqualityAccessor = laccessorToEqulity.get(accessor);
        if (listDeepEqualityAccessor == null) {
            listDeepEqualityAccessor = new ListDeepEqualityAccessor();
            laccessorToEqulity.put(accessor, listDeepEqualityAccessor);
        }

        try {
            arg.second = listDeepEqualityAccessor.accessList(accessor, arg.first, this);
        } catch (Exception e) {
            throw new AsterixException(e);
        }

        return null;
    }

    @Override public Void visit(ARecordVisitablePointable accessor, Pair<IVisitablePointable, Boolean> arg)
            throws AsterixException {
        RecordDeepEqualityAccessor recDeepEqualityAccessor = raccessorToEquality.get(accessor);
        if (recDeepEqualityAccessor == null) {
            recDeepEqualityAccessor = new RecordDeepEqualityAccessor();
            raccessorToEquality.put(accessor, recDeepEqualityAccessor);
        }

        try {
            arg.second = recDeepEqualityAccessor.accessRecord(accessor, arg.first, this);
        } catch (Exception e) {
            throw new AsterixException(e);
        }

        return null;
    }

    @Override public Void visit(AFlatValuePointable accessor,Pair<IVisitablePointable, Boolean> arg)
            throws AsterixException {

        if (accessor.equals(arg.first)) {
            arg.second = true;
            return null;
        }
        try {
            ATypeTag tt1 = PointableUtils.getTypeTag(accessor);
            ATypeTag tt2 = PointableUtils.getTypeTag(arg.first);

            if (tt1 != tt2) {
                if (!ATypeHierarchy.isSameTypeDomain(tt1, tt2, false)) {
                    arg.second = false;
                } else {
                    // If same domain, check if numberic
                    Domain domain = ATypeHierarchy.getTypeDomain(tt1);
                    byte b1[] = accessor.getByteArray();
                    byte b2[] = arg.first.getByteArray();
                    if (domain == Domain.NUMERIC) {
                        int s1 = accessor.getStartOffset();
                        int s2 = arg.first.getStartOffset();
                        arg.second = (ATypeHierarchy.getDoubleValue(b1, s1) == ATypeHierarchy.getDoubleValue(b2, s2));
                    } else {
                        arg.second = false;
                    }
                }
            } else {
                arg.second = PointableUtils.byteArrayEqual(accessor, arg.first, 1);
            }
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }

        return null;
    }

}
