package edu.uci.ics.asterix.runtime.evaluators.visitors;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.pointables.AFlatValuePointable;
import edu.uci.ics.asterix.om.pointables.AListPointable;
import edu.uci.ics.asterix.om.pointables.ARecordPointable;
import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.runtime.evaluators.functions.PointableUtils;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IValueReference;

import java.util.HashMap;
import java.util.Map;

public class DeepEqualityVisitor implements IVisitablePointableVisitor<Void, Pair<IVisitablePointable, Boolean>> {
    private final Map<IVisitablePointable, ListDeepEqualityAccessor> laccessorToEqulity =
            new HashMap<IVisitablePointable, ListDeepEqualityAccessor>();
    private final Map<IVisitablePointable, RecordDeepEqualityAccessor> raccessorToEquality =
            new HashMap<IVisitablePointable, RecordDeepEqualityAccessor>();


    @Override public Void visit(AListPointable accessor, Pair<IVisitablePointable, Boolean> arg)
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

    @Override public Void visit(ARecordPointable accessor, Pair<IVisitablePointable, Boolean> arg)
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

        ATypeTag tt1 = getTypeTag(accessor);
        ATypeTag tt2 = getTypeTag(arg.first);

        if (accessor.equals(arg.second)) {
            arg.second = true;
            return null;
        }

        if(tt1!=tt2 || accessor.getLength()!=arg.first.getLength()) {
            arg.second = false;
            return null;
        }

        try {
            arg.second = byteArrayEqual(accessor, arg.first, 1);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }

        return null;
    }

    public boolean byteArrayEqual(IValueReference valueRef1, IValueReference valueRef2, int dataOffset) throws
            HyracksDataException {
        return PointableUtils.byteArrayEqual(valueRef1, valueRef2, dataOffset);
    }

    public ATypeTag getTypeTag(IVisitablePointable visitablePointable) {
        return PointableUtils.getTypeTag(visitablePointable);
    }

}
