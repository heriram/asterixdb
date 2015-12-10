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
package org.apache.asterix.om.typecomputer.impl;

import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeHelper;
import org.apache.asterix.om.util.admdebugger.AnnotatedFieldNameComputerUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class AdmToBytesTypeComputer implements IResultTypeComputer {
    public static final AdmToBytesTypeComputer INSTANCE = new AdmToBytesTypeComputer();
    private final long VARIABLE_OUTPULEVEL = -1;
    private long outputLevel = 0;

    private AdmToBytesTypeComputer() {
    }

    public static AbstractCollectionType extractListType(IAType type) {
        if (type.getTypeTag() == ATypeTag.UNORDEREDLIST)
            return TypeComputerUtils.extractUnorderedListType(type);
        else
            return TypeComputerUtils.extractOrderedListType(type);
    }

    private long getLevel(IAType inputType1, ILogicalExpression expr) throws AlgebricksException {
        // Treat null as a level = 0
        if (inputType1.getTypeTag() == ATypeTag.NULL) {
            return 0;
        }

        // If input level is a variable
        if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            return VARIABLE_OUTPULEVEL;
        }

        if (expr.getExpressionTag() != LogicalExpressionTag.CONSTANT)
            throw new AlgebricksException("Expected a variable or either an integer or a string (INF) constant"
                    + " but got a " + expr.getExpressionTag());

        ConstantExpression constExpr = (ConstantExpression) expr;
        IAObject value = ((AsterixConstantValue) constExpr.getValue()).getObject();

        switch (inputType1.getTypeTag()) {
            case INT64:
                return ((AInt64) value).getLongValue();
            case STRING:
                String valStr = ((AString) value).getStringValue();
                if (valStr.equals("INF")) { // Check if infinity
                    return Long.MAX_VALUE;
                } else { // We accept also numbers expressed as string
                    for (int i = 0; i < valStr.length(); i++) {
                        if (!Character.isDigit(valStr.charAt(i))) {
                            throw new AlgebricksException("Invalid number. Expected argument is a number from 0 "
                                    + "to INF, where INF is infinity");
                        }
                    }
                    return Long.parseLong(valStr);
                }
            default:
                throw new AlgebricksException("Invalid argument. Expected a number from 0 "
                        + "to INF, where INF is infinity, but got " + inputType1);
        }
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expression;

        ILogicalExpression expr1 = funcExpr.getArguments().get(1).getValue();
        IAType type1 = (IAType) env.getType(expr1);
        outputLevel = getLevel(type1, expr1);

        // If level is unknown at translation time (input is a variable)
        if (outputLevel == VARIABLE_OUTPULEVEL) {
            IAType resultType = DefaultOpenFieldType.getDefaultOpenFieldType(type1.getTypeTag());
            if (resultType == null) {
                return DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
            }
            return resultType;
        }

        // If for printing of the raw bytes only
        if (outputLevel == 0) {
                return new ARecordType("RawBytes", new String[] { "RawBytes" }, new IAType[] { BuiltinType.ASTRING },
                        false);
        }

        // For printing annotated byte arrays (outputLevel > 0)
        return computeResultType((IAType) env.getType(funcExpr.getArguments().get(0).getValue()), 1);
    }

    private IAType computeResultType(IAType inType, long nestedLevel) throws AlgebricksException {
        IAType inputType = inType;
        IAType resultType;

        if (TypeHelper.canBeNull(inputType)) {
            return AUnionType.createNullableType(AnnotatedFieldNameComputerUtil
                    .getNullableAnnotatedFieldsRecordType(true));
        }

        // If the input type is not a record or level = 1, not a complex object or reached the lowest nesting level we
        // print out the annotated byte with no nesting level processing
        if (outputLevel == 1 || !inputType.getTypeTag().isDerivedType() || nestedLevel == outputLevel) {
            resultType = AnnotatedFieldNameComputerUtil.getAnnotatedBytesRecordType(inputType.getTypeTag());
        } else { // Is a list or a record and outputLevel > 1
            switch (inputType.getTypeTag()) {
                case RECORD:
                    resultType = getResultRecordType(inputType, nestedLevel);
                    break;
                case UNION:
                    resultType = computeResultType(inputType, nestedLevel);
                    break;
                default: //an UNORDEREDLIST or ORDEREDLIST
                    resultType = getResultListType(inputType, nestedLevel);
            }
        }

        return resultType;
    }

    private IAType getResultListType(IAType inputType, long nestedLevel) throws AlgebricksException {
        AbstractCollectionType inputListType = extractListType(inputType);
        if (inputListType.getItemType().getTypeTag() == ATypeTag.ANY || inputListType == null) {
            return creatListType(inputType.getTypeTag(), BuiltinType.ANY, "annotated-bytes-open-type");
        }
        return getNestedList(inputListType, nestedLevel);
    }

    private IAType getNestedList(AbstractCollectionType inputListType, long nestedLevel) throws AlgebricksException {
        IAType inputItemType = inputListType.getItemType();
        ATypeTag inputItemTypeTypeTag = inputItemType.getTypeTag();
        IAType reqItemType;
        if (inputItemTypeTypeTag.isDerivedType() && nestedLevel < outputLevel) {
            reqItemType = computeResultType(inputItemType, nestedLevel + 1);
        } else {
            reqItemType = AnnotatedFieldNameComputerUtil.getAnnotatedBytesRecordType(inputItemType.getTypeTag());
        }
        return creatListType(inputListType.getTypeTag(), reqItemType, "annotated-bytes(" + inputListType.getTypeName()
                + ")");
    }

    private IAType creatListType(ATypeTag typeTag, IAType itemType, String typeName) throws AlgebricksException {
        switch (typeTag) {
            case ORDEREDLIST:
                return new AOrderedListType(itemType, typeName);
            case UNORDEREDLIST:
                return new AUnorderedListType(itemType, typeName);
            default:
                throw new AlgebricksException("Not a valid AsterixDB list");
        }
    }

    private IAType getResultRecordType(IAType inputType, long nestedLevel) throws AlgebricksException {
        ARecordType inputRecordType = TypeComputerUtils.extractRecordType(inputType);
        if (inputRecordType == null) {
            return DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
        }
        return getNestedRecordFields(inputRecordType, nestedLevel);
    }

    private ARecordType getNestedRecordFields(ARecordType inputType, long nestedLevel) throws AlgebricksException {
        String fieldNames[] = inputType.getFieldNames();
            IAType fieldTypes[] = inputType.getFieldTypes();

            IAType[] newTypes = new IAType[fieldNames.length];
            for (int i = 0; i < fieldTypes.length; i++) {
                ATypeTag fieldTypeTag = fieldTypes[i].getTypeTag();
                if (fieldTypeTag.isDerivedType() && nestedLevel < outputLevel) {
                    newTypes[i] = computeResultType(fieldTypes[i], nestedLevel + 1);
                } else {
                    newTypes[i] = AnnotatedFieldNameComputerUtil
                            .getAnnotatedBytesRecordType(fieldTypes[i].getTypeTag());
                }
            }
            String typeName = "annotated-bytes(" + inputType.getTypeName() + ")";
            return new ARecordType(typeName, fieldNames, newTypes, inputType.isOpen());
    }
}
