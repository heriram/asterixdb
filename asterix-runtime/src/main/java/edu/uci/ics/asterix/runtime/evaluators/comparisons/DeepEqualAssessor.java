package edu.uci.ics.asterix.runtime.evaluators.comparisons;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.runtime.evaluators.functions.PointableUtils;
import edu.uci.ics.asterix.runtime.evaluators.visitors.DeepEqualityVisitor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;

/**
 *
 * Use {@link DeepEqualityVisitor} to assess the deep equality between two
 * pointable values, including oredered and unordered lists, record values, etc.
 *
 * Example:  Let IVisitablePointable accessor0, IVisitablePointable accessor1 be two
 *      value references. To assess their equality, simply use
 *
 *      boolean isEqual = DeepEqualAssessor.INSTANCE.isEqual(accessor0, accessor1);
 *
 */

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
