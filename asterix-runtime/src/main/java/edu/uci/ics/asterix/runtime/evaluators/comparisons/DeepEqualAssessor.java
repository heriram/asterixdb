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
