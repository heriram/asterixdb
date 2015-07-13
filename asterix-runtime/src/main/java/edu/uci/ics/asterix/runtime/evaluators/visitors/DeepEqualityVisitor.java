/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.runtime.evaluators.visitors;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.pointables.AFlatValuePointable;
import edu.uci.ics.asterix.om.pointables.AListVisitablePointable;
import edu.uci.ics.asterix.om.pointables.ARecordVisitablePointable;
import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.runtime.evaluators.functions.PointableUtils;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

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

        ATypeTag tt1 = PointableUtils.getTypeTag(accessor);
        ATypeTag tt2 = PointableUtils.getTypeTag(arg.first);

        if (accessor.equals(arg.second)) {
            arg.second = true;
            return null;
        }

        if(tt1!=tt2 || accessor.getLength()!=arg.first.getLength()) {
            arg.second = false;
            return null;
        }

        try {
            arg.second = PointableUtils.byteArrayEqual(accessor, arg.first, 1);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }

        return null;
    }
}
