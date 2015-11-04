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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;

public class AdmToBytesTypeComputer implements IResultTypeComputer {
    private static final long serialVersionUID = 1L;

    public static final AdmToBytesTypeComputer INSTANCE = new AdmToBytesTypeComputer();

    private long maxLevel = 0;

    private AdmToBytesTypeComputer() {
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expression;
        maxLevel = getLevel(env, funcExpr.getArguments().get(1).getValue());
        IAType intputType = (IAType) env.getType((AbstractLogicalExpression)funcExpr.getArguments().get(0).getValue());

        if (maxLevel==0) { // Output type is an array of byte (raw byte dump
            return new AOrderedListType(BuiltinType.AINT16, null);
        } else {
            if (intputType.getTypeTag().equals(ATypeTag.RECORD))
                return getResultRecordType(intputType);
            else {
                return getAnnotatedBytesRecordType();
            }
        }
    }


    private IAType getResultRecordType(IAType type) throws AlgebricksException {
        ARecordType inputRecordType = TypeComputerUtils.extractRecordType(type);
        if (inputRecordType == null) {
            return DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
        }
        return getNestedRecordFields(inputRecordType, 0);
    }

    private ARecordType getNestedRecordFields(ARecordType inputType, long level) throws AlgebricksException {
        String fieldNames[] = inputType.getFieldNames();
        IAType fieldTypes[] = inputType.getFieldTypes();

        IAType[] newTypes = new IAType[fieldNames.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            if (fieldTypes[i].getTypeTag() == ATypeTag.RECORD && (level>1 && level<maxLevel)) {
                newTypes[i] = getNestedRecordFields((ARecordType) fieldTypes[i], level + 1);
            } else {
                newTypes[i] = getAnnotatedBytesRecordType();
            }
        }
        try {
            String typeName = "annotated(" + inputType.getTypeName() + ")";
            return new ARecordType(typeName, fieldNames, newTypes, inputType.isOpen());
        } catch (AsterixException | HyracksException e) {
            throw new AlgebricksException(e);
        }
    }

    private AbstractCollectionType getNestedList(IAType type)
            throws AlgebricksException {
        AbstractCollectionType inputListType = extractListType(type);
        AbstractCollectionType resultListType;
        IAType resultItemType;
        ATypeTag inputItemType = inputListType.getItemType().getTypeTag();

        // We have a complex object
        if (inputItemType.isDerivedType() || inputListType == null) {
            resultListType = DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE;
            if(type.getTypeTag() == ATypeTag.UNORDEREDLIST) {
                resultListType = DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE;
            }
            resultListType.setTypeName("annotated-bytes");
        } else {
            resultItemType = getAnnotatedBytesRecordType();
            resultListType = new AOrderedListType(resultItemType, resultItemType.getTypeName());
            if (type.getTypeTag()==ATypeTag.UNORDEREDLIST) {
                resultListType = new AUnorderedListType(resultItemType, resultItemType.getTypeName());
            }
        }
        return resultListType;
    }


    private ARecordType getAnnotatedBytesRecordType() throws AlgebricksException {
        String fieldNames[] = {"tag", "length", "value"};
        IAType fieldTypes[] = { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING};
        try {
            return new ARecordType("ByteArrayfields", fieldNames, fieldTypes, true);
        } catch (HyracksDataException | AsterixException e) {
            throw new AlgebricksException("Cannot create annotated record type instance.");
        }
    }

    private static long getLevel(IVariableTypeEnvironment env, ILogicalExpression expr) throws AlgebricksException {
        if (expr.getExpressionTag() != LogicalExpressionTag.CONSTANT)
            throw new AlgebricksException("Expected a constant either an integer or a string (INF)"
                    + " but got a " + expr.getExpressionTag());

        IAType type1 = (IAType) env.getType(expr); // For max level
        ConstantExpression constExpr = (ConstantExpression) expr;
        IAObject value = ((AsterixConstantValue) constExpr.getValue()).getObject();

        switch (type1.getTypeTag()) {
            case INT64:
                return ((AInt64)value).getLongValue();
            case STRING:
                String valStr = ((AString)value).getStringValue();
                if (valStr.equals("INF")) { // Check if infinity
                    return Long.MAX_VALUE;
                } else { // We accept also numbers expressed as string
                    for(int i=0;i<valStr.length();i++)
                    {
                        if( !Character.isDigit(valStr.charAt(i)) ) {
                            throw new AlgebricksException("Invalid number. Expected argument is a number from 0 "
                                    + "to INF, where INF is infinity");
                        }
                    }
                    return Long.parseLong(valStr);
                }
            default:
                throw new AlgebricksException("Invalid argument. Expected a number from 0 "
                        + "to INF, where INF is infinity, but got " + type1);
        }
    }

    public static AbstractCollectionType extractListType (IAType type) {
        if(type.getTypeTag()==ATypeTag.UNORDEREDLIST)
            return TypeComputerUtils.extractUnorderedListType(type);
        else return TypeComputerUtils.extractOrderedListType(type);
    }
}
