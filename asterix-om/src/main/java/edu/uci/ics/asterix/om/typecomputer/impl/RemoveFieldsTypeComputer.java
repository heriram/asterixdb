package edu.uci.ics.asterix.om.typecomputer.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.base.AOrderedList;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class RemoveFieldsTypeComputer extends AbstractRecordManipulationTypeComputer {

    private static final long serialVersionUID = 1L;

    public static final RemoveFieldsTypeComputer INSTANCE = new RemoveFieldsTypeComputer();

    private RemoveFieldsTypeComputer() {
    }

    private final Set<String> removeSet = new HashSet<String>();

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {

        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;
        IAType inputRecType = (IAType) env.getType(f.getArguments().get(0).getValue());
        IAType inputListType = (IAType) env.getType(f.getArguments().get(1).getValue());
        ARecordType inputRecordType = extractRecordType(inputRecType);
        AOrderedListType inputOrderedListType = extractOrderedListType(inputListType);

        if (inputRecordType == null) {
            throw new AlgebricksException(
                    "The function 'remove-fields' expects a record as the first argument, but got " + inputRecType);
        }

        if (inputOrderedListType == null) {
            throw new AlgebricksException(
                    "The function 'remove-fields' expects an ordered list as the second argument, but got "
                            + inputListType);
        }

        ILogicalExpression exp = f.getArguments().get(1).getValue();
        AOrderedList removeList = null;
        if (exp.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            ConstantExpression listExpr = (ConstantExpression) exp;
            if (!(listExpr.getValue() instanceof AsterixConstantValue)) {
                throw new AlgebricksException("Expecting a list of strings and found " + listExpr.getValue()
                        + " instead.");
            }
            removeList = (AOrderedList) ((AsterixConstantValue) listExpr.getValue()).getObject();
            removeSet.clear();
            for (int i = 0; i < removeList.size(); i++) {
                AString s = (AString) removeList.getItem(i);
                removeSet.add(s.getStringValue());
            }
        }

        IAType resultType = null;
        List<String> resultFieldNames = new ArrayList<>();
        List<IAType> resultFieldTypes = new ArrayList<>();

        for (String fieldName : inputRecordType.getFieldNames()) {
            try {
                if (!removeSet.contains(fieldName)) {
                    resultFieldNames.add(fieldName);
                    if (inputRecordType.getFieldType(fieldName).getTypeTag() == ATypeTag.RECORD) {
                        ARecordType nestedType = (ARecordType) inputRecordType.getFieldType(fieldName);
                        //Deep Copy prevents altering of input types
                        resultFieldTypes.add(nestedType.deepCopy(nestedType));
                    } else {

                        resultFieldTypes.add(inputRecordType.getFieldType(fieldName));

                    }
                }
            } catch (IOException e) {
                throw new AlgebricksException(e);
            }
        }

        int n = resultFieldNames.size();
        String resultTypeName = "result-record(" + inputRecordType.getTypeName() + ")";
        try {
            resultType = new ARecordType(resultTypeName, resultFieldNames.toArray(new String[n]),
                    resultFieldTypes.toArray(new IAType[n]), inputRecordType.isOpen());
        } catch (HyracksDataException | AsterixException e) {
            throw new AlgebricksException(e);
        }

        return resultType;
    }

}
