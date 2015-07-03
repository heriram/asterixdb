package edu.uci.ics.asterix.om.typecomputer.impl;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.types.TypeHelper;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import org.apache.commons.lang3.mutable.Mutable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RecordAddFieldsTypeComputer extends AbstractRecordManipulationTypeComputer {
    private static final long serialVersionUID = 1L;

    public static final RecordAddFieldsTypeComputer INSTANCE = new RecordAddFieldsTypeComputer();

    private static final String FIELD_NAME_NAME = "field-name";
    private static final String FIELD_VALUE_VALUE = "field-value";

    private RecordAddFieldsTypeComputer() {
    }


    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expression;
        IAType type0 = (IAType) env.getType(funcExpr.getArguments().get(0).getValue());

        ARecordType inputRecordType = extractRecordType(type0);
        if (inputRecordType == null) {
            return BuiltinType.ANY;
        }

        AbstractLogicalExpression arg1 = (AbstractLogicalExpression) funcExpr.getArguments().get(1).getValue();
        IAType type1 = (IAType) env.getType(arg1);
        AOrderedListType inputOrderedListType = extractOrderedListType(type1);
        if (inputOrderedListType == null) {
            return inputRecordType;
        }

        boolean nullable = TypeHelper.canBeNull(type0) || TypeHelper.canBeNull(type1);

        List<String> additionalFieldNames = getStringListFromPool();
        List<IAType> additionalFieldTypes = getATypeListFromPool();
        List<String> resultFieldNames = getStringListFromPool();
        List<IAType> resultFieldTypes = getATypeListFromPool();

        resultFieldNames.addAll(Arrays.asList(inputRecordType.getFieldNames()));
        Collections.sort(resultFieldNames);

        for (String fieldName : resultFieldNames) {
            try {
                if (inputRecordType.getFieldType(fieldName).getTypeTag() == ATypeTag.RECORD) {
                    ARecordType nestedType = (ARecordType) inputRecordType.getFieldType(fieldName);
                    //Deep Copy prevents altering of input types
                    resultFieldTypes.add(nestedType.deepCopy(nestedType));
                } else {
                    resultFieldTypes.add(inputRecordType.getFieldType(fieldName));
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) arg1;
        List<Mutable<ILogicalExpression>> args = f.getArguments();

        // Iterating through the orderlist input
        for (Mutable<ILogicalExpression> arg: args) {
            AbstractFunctionCallExpression recConsExpr = (AbstractFunctionCallExpression) arg.getValue();
            ARecordType rtype = extractRecordType((IAType) env.getType(recConsExpr));
            if (rtype!= null) {
                String[] fn = rtype.getFieldNames();
                IAType[] ft = rtype.getFieldTypes();
                for (int j=0; j<fn.length; j++) {
                    if (fn[j].equals(FIELD_NAME_NAME)) {
                        ILogicalExpression fieldNameExpr = recConsExpr.getArguments().get(j).getValue();
                        switch (fieldNameExpr.getExpressionTag()) {
                            case CONSTANT: // Top fields only
                                AString as = (AString) ((AsterixConstantValue) ((ConstantExpression) fieldNameExpr).getValue()).getObject();
                                if (as.getType().getTypeTag() == ATypeTag.STRING) {
                                    // Get the actual "field-name" string
                                    ILogicalExpression recFieldExpr = recConsExpr.getArguments().get(j + 1).getValue();
                                    if (recFieldExpr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                                        as = (AString) ((AsterixConstantValue) ((ConstantExpression) recFieldExpr)
                                                .getValue()).getObject();
                                        additionalFieldNames.add(as.getStringValue());
                                    }
                                }
                                break;
                            default:
                                throw new AlgebricksException(fieldNameExpr + " is not supported.");

                        }
                    } else if (fn[j].equals(FIELD_VALUE_VALUE)) {
                        additionalFieldTypes.add(ft[j]);
                    }

                }

            }
        }

        if (additionalFieldNames.size()>0) {
            addAdditionalFields(resultFieldNames, resultFieldTypes, additionalFieldNames, additionalFieldTypes);
        }

        returnStringListToPool(additionalFieldNames);
        returnATypeListToPool(additionalFieldTypes);

        String resultTypeName =
                "appended(" + inputRecordType.getTypeName() + ", " + inputOrderedListType.getTypeName() + ")";
        IAType resultType;
        try {
            int n = resultFieldNames.size();
            resultType = new ARecordType(resultTypeName, resultFieldNames.toArray(new String[n]),
                    resultFieldTypes.toArray(new IAType[n]), true);
        } catch (AsterixException | HyracksDataException e) {
            throw new AlgebricksException(e);
        }
        if (nullable) {
            resultType = AUnionType.createNullableType(resultType);
        }

        returnStringListToPool(resultFieldNames);
        returnATypeListToPool(resultFieldTypes);


        return resultType;
    }


}
