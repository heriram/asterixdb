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
package org.apache.asterix.runtime.evaluators.comparisons;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy.Domain;
import org.apache.asterix.runtime.evaluators.functions.PointableUtils;
import org.apache.asterix.runtime.evaluators.visitors.DeepEqualityVisitor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;

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

        if (vp0.equals(vp1))
            return true;

        ATypeTag tt0 = PointableUtils.getTypeTag(vp0);
        ATypeTag tt1 = PointableUtils.getTypeTag(vp1);

        if (tt0 != tt1) {
            // If types are numeric compare their real values instead
            if (ATypeHierarchy.isSameTypeDomain(tt0, tt1, false) &&
                    ATypeHierarchy.getTypeDomain(tt0) == Domain.NUMERIC) {
                try {
                    double val0 = ATypeHierarchy.getDoubleValue(vp0.getByteArray(), vp0.getStartOffset());
                    double val1 = ATypeHierarchy.getDoubleValue(vp1.getByteArray(), vp1.getStartOffset());
                    if (val0 == val1) return true;
                    else return false;
                } catch (HyracksDataException e) {
                    throw new AlgebricksException(e);
                }

            }
            else
                return false;
        }

        final Pair<IVisitablePointable, Boolean> arg =
                new Pair<IVisitablePointable, Boolean>(vp1, Boolean.FALSE);

        // Assess the nested equality
        vp0.accept(equalityVisitor, arg);

        return arg.second;
    }
}
