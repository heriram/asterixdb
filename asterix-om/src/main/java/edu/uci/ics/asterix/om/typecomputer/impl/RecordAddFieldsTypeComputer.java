package edu.uci.ics.asterix.om.typecomputer.impl;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.types.*;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import org.apache.commons.lang3.ArrayUtils;
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
                        ILogicalExpression recExpr = recConsExpr.getArguments().get(j).getValue();
                        if (recExpr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                            AString as = (AString) ((AsterixConstantValue) ((ConstantExpression) recExpr)
                                    .getValue()).getObject();
                            if (as.getType().getTypeTag() == ATypeTag.STRING) {
                                ILogicalExpression recFieldExpr = recConsExpr.getArguments().get(j+1).getValue();
                                if (recFieldExpr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                                    as = (AString) ((AsterixConstantValue) ((ConstantExpression) recFieldExpr)
                                            .getValue()).getObject();
                                    additionalFieldNames.add(as.getStringValue());
                                }
                            }

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


    private void addAdditionalFields(List<String> resultFieldNames, List<IAType> resultFieldTypes,
            List<String> additionalFieldNames , List<IAType> additionalFieldTypes) throws AlgebricksException {

        for (int i=0; i<additionalFieldNames.size(); i++) {
            String fn = additionalFieldNames.get(i);
            IAType ft = additionalFieldTypes.get(i);
            int pos = Collections.binarySearch(resultFieldNames, fn);
            if (pos >= 0) {
                try {
                    resultFieldTypes.set(pos, mergedNestedType(ft, resultFieldTypes.get(pos)));
                } catch (AsterixException e) {
                    throw new AlgebricksException(e);
                }

            } else {
                resultFieldNames.add(fn);
                resultFieldTypes.add(ft);
            }
        }
    }


    private IAType mergedNestedType(IAType fieldType1, IAType fieldType0) throws AlgebricksException, AsterixException {
        if (fieldType1.getTypeTag() != ATypeTag.RECORD || fieldType0.getTypeTag() != ATypeTag.RECORD) {
            throw new AlgebricksException("Duplicate field " + fieldType1.getTypeName() + " encountered");
        }

        ARecordType returnType = (ARecordType) fieldType0;
        ARecordType fieldType1Copy = (ARecordType) fieldType1;

        for (int i = 0; i < fieldType1Copy.getFieldTypes().length; i++) {
            try {
                int pos = returnType.findFieldPosition(fieldType1Copy.getFieldNames()[i]);
                if (pos >= 0) {
                    if (fieldType1Copy.getFieldTypes()[i].getTypeTag() != ATypeTag.RECORD) {
                        break;
                    }
                    IAType[] oldTypes = returnType.getFieldTypes();
                    oldTypes[pos] = mergedNestedType(fieldType1Copy.getFieldTypes()[i], returnType.getFieldTypes()[pos]);
                    returnType = new ARecordType(returnType.getTypeName(), returnType.getFieldNames(), oldTypes,
                            returnType.isOpen());
                } else {
                    IAType[] combinedFieldTypes = ArrayUtils
                            .addAll(returnType.getFieldTypes().clone(), fieldType1Copy.getFieldTypes()[i]);
                    returnType = new ARecordType(returnType.getTypeName(), ArrayUtils.addAll(
                            returnType.getFieldNames(), fieldType1Copy.getFieldNames()[i]), combinedFieldTypes,
                            returnType.isOpen());
                }

            } catch (IOException | AsterixException e) {
                throw new AlgebricksException(e);
            }
        }

        return returnType;
    }

}
