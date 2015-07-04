package edu.uci.ics.asterix.runtime.evaluators.functions;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.runtime.evaluators.visitors.DeepEqualityVisitor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;

public class DeepEqualAssessor {

    private static final long serialVersionUID = 1L;

    public static final DeepEqualAssessor INSTANCE = new DeepEqualAssessor();


    private DeepEqualAssessor() {
    }

    private final DeepEqualityVisitor equalityVisitor = new DeepEqualityVisitor();

    public boolean isEqual(IVisitablePointable vp0, IVisitablePointable vp1)
            throws AlgebricksException, AsterixException {

        if (vp0 == null || vp1 == null)
            return false;

        if (PointableUtils.getTypeTag(vp0) != PointableUtils.getTypeTag(vp1))
            return false;

        if (vp0.equals(vp1))
            return true;

        final Pair<IVisitablePointable, Boolean> arg =
                new Pair<IVisitablePointable, Boolean>(vp1, Boolean.FALSE);

        // Assess the nested equality
        vp0.accept(equalityVisitor, arg);

        return arg.second;
    }
}
