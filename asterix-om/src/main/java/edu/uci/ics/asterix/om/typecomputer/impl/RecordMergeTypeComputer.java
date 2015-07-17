/*
 * Copyright 2009-2013 by The Regents of the University of California
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.uci.ics.asterix.om.typecomputer.impl;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.types.TypeHelper;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RecordMergeTypeComputer extends AbstractRecordManipulationTypeComputer {
    private static final long serialVersionUID = 1L;

    public static final RecordMergeTypeComputer INSTANCE = new RecordMergeTypeComputer();

    private RecordMergeTypeComputer() {
    }

    public static ARecordType extractRecordType(IAType t) {
        if (t.getTypeTag() == ATypeTag.RECORD) {
            return (ARecordType) t;
        }

        if (t.getTypeTag() == ATypeTag.UNION) {
            IAType innerType = ((AUnionType) t).getUnionList().get(1);
            if (innerType.getTypeTag() == ATypeTag.RECORD) {
                return (ARecordType) innerType;
            }
        }

        return null;
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;
        IAType t0 = (IAType) env.getType(f.getArguments().get(0).getValue());
        IAType t1 = (IAType) env.getType(f.getArguments().get(1).getValue());
        boolean nullable = TypeHelper.canBeNull(t0) || TypeHelper.canBeNull(t1);
        ARecordType recType0 = extractRecordType(t0);
        ARecordType recType1 = extractRecordType(t1);

        if (recType0 == null || recType1 == null) {
            throw new AlgebricksException("record-merge expects possibly NULL records as arguments, but got (" + t0
                    + ", " + t1 + ")");
        }

        List<String> resultFieldNames = new ArrayList<>();
        for (String fieldName : recType0.getFieldNames()) {
            resultFieldNames.add(fieldName);
        }
        Collections.sort(resultFieldNames);
        List<IAType> resultFieldTypes = new ArrayList<>();
        for (String fieldName : resultFieldNames) {
            try {
                if (recType0.getFieldType(fieldName).getTypeTag() == ATypeTag.RECORD) {
                    ARecordType nestedType = (ARecordType) recType0.getFieldType(fieldName);
                    //Deep Copy prevents altering of input types
                    resultFieldTypes.add(nestedType.deepCopy(nestedType));
                } else {
                    resultFieldTypes.add(recType0.getFieldType(fieldName));
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        List<String> additionalFieldNames = new ArrayList<>();
        List<IAType> additionalFieldTypes = new ArrayList<>();
        String fieldNames[] = recType1.getFieldNames();
        IAType fieldTypes[] = recType1.getFieldTypes();
        for (int i = 0; i < fieldNames.length; ++i) {
            int pos = Collections.binarySearch(resultFieldNames, fieldNames[i]);
            if (pos >= 0) {
                IAType rt = resultFieldTypes.get(pos);
                if (rt.getTypeTag() != fieldTypes[i].getTypeTag()) {
                    throw new AlgebricksException("Duplicate field " + fieldNames[i] + " encountered");
                }
                try {
                    if(fieldTypes[i].getTypeTag() == ATypeTag.RECORD && rt.getTypeTag() == ATypeTag.RECORD) {
                        resultFieldTypes.set(pos, mergedNestedType(fieldTypes[i], rt));
                    }
                } catch (AsterixException e) {
                    throw new AlgebricksException(e);
                }

            } else {
                additionalFieldNames.add(fieldNames[i]);
                additionalFieldTypes.add(fieldTypes[i]);
            }
        }

        resultFieldNames.addAll(additionalFieldNames);
        resultFieldTypes.addAll(additionalFieldTypes);
        String resultTypeName = "merged(" + recType0.getTypeName() + ", " + recType1.getTypeName() + ")";
        boolean isOpen = recType0.isOpen() || recType1.isOpen();
        IAType resultType = null;
        try {
            resultType = new ARecordType(resultTypeName, resultFieldNames.toArray(new String[] {}),
                    resultFieldTypes.toArray(new IAType[] {}), isOpen);
        } catch (AsterixException | HyracksDataException e) {
            throw new AlgebricksException(e);
        };

        if (nullable) {
            resultType = AUnionType.createNullableType(resultType);
        }
        return resultType;
    }

}
