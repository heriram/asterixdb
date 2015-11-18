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
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.admdebugger.FieldTypeComputerUtils;
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

    private long outputLevel = 0;

    private final long NESTED_LEVEL_OFFSET = 2;

    private AdmToBytesTypeComputer() {
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expression;

        outputLevel = getLevel(env, funcExpr.getArguments().get(1).getValue());


        // If level is unknown at translation time (input is a variable)
        if (outputLevel == -1) {
            return DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
        }

        // If we want to print out the raw bytes
        if (outputLevel == 0){
            try {
                return new ARecordType("RawBytes", new String[]{"RawBytes"}, new IAType[]{BuiltinType.ASTRING}, false);
            } catch (HyracksDataException | AsterixException e) {
                throw new AlgebricksException(e);
            }
        }

        IAType intputType = (IAType) env.getType((AbstractLogicalExpression)funcExpr.getArguments().get(0).getValue());
        ATypeTag typeTag = intputType.getTypeTag();

        // If the input type is not a record or level = 1 we print out
        // the annotated byte with no nesting level processing
        if (outputLevel == 1 || typeTag != ATypeTag.RECORD) {
            return FieldTypeComputerUtils.getAnnotatedBytesRecordType(typeTag);
        } else {
            return getResultRecordType(intputType);
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
        try {
            String fieldNames[] = inputType.getFieldNames();
            IAType fieldTypes[] = inputType.getFieldTypes();

            IAType[] newTypes = new IAType[fieldNames.length];
            for (int i = 0; i < fieldTypes.length; i++) {
                if (fieldTypes[i].getTypeTag() == ATypeTag.RECORD && outputLevel > NESTED_LEVEL_OFFSET && level<outputLevel) {
                    newTypes[i] = getNestedRecordFields((ARecordType) fieldTypes[i], level + 1);
                } else {
                    newTypes[i] = FieldTypeComputerUtils.getAnnotatedBytesRecordType(fieldTypes[i].getTypeTag());//DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
                }
            }
            StringBuilder typeName = new StringBuilder("annotated");
            if(inputType.getTypeName()!=null)
                typeName.append("(" + inputType.getTypeName() + ")");
            return new ARecordType(typeName.toString(), fieldNames, newTypes, inputType.isOpen());

        } catch (AsterixException | HyracksException e) {
            throw new AlgebricksException(e);
        }
    }


    public static long getLevel(IVariableTypeEnvironment env, ILogicalExpression expr) throws AlgebricksException {
        IAType type1 = (IAType) env.getType(expr);

        // Treat null as a level = 0
        if (type1.getTypeTag() == ATypeTag.NULL) {
            return 0;
        }

        // If input level is a variable
        if ( expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            return -1;
        }

        if (expr.getExpressionTag() != LogicalExpressionTag.CONSTANT)
            throw new AlgebricksException("Expected a variable or either an integer or a string (INF) constant"
                    + " but got a " + expr.getExpressionTag());

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
}
