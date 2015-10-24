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
import org.apache.asterix.om.base.AInt16;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class AdmToBytesTypeComputer implements IResultTypeComputer {
    private static final long serialVersionUID = 1L;

    public static final AdmToBytesTypeComputer INSTANCE = new AdmToBytesTypeComputer();

    private ARecordType fieldRecordType;

    private AdmToBytesTypeComputer() {
        String arfName[] = new String[]{"tag", "length", "value"};
        IAType arfByteArray[] = new IAType[]{ BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING};
        try {
            fieldRecordType = new ARecordType("byteArrayfield", arfName, arfByteArray, true);
        } catch (AsterixException | HyracksDataException e) {
            e.printStackTrace();
        }
    }

    @Override public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {

        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;

        // Check Input argument specifying the level
        long inputNumber = 0L;
        ILogicalExpression arg1Expr = f.getArguments().get(1).getValue();
        if (arg1Expr.getExpressionTag()== LogicalExpressionTag.CONSTANT) {
            IAType arg1Type = (IAType) env.getType(arg1Expr);
            switch(arg1Type.getTypeTag()) {
                case INT32:
                case INT64:
                case INT16:
                    inputNumber = getNumberValue ((ConstantExpression) arg1Expr);
                    if (inputNumber<0) { // Accepting only positive numbers
                        throw new AlgebricksException("Invalid number. Expected argument is a number from 0 "
                                + "to INF, where INF is infinity");
                    }
                    break;
                case STRING:
                    AString as = (AString) ((AsterixConstantValue) ((ConstantExpression) arg1Expr).getValue()).getObject();
                    if (as.getStringValue().equals("INF")) { // Check if infinity
                        inputNumber = Long.MAX_VALUE;
                    } else { // We accept also numbers expressed as string
                        String sv = as.getStringValue();
                        for(int i=0;i<sv.length();i++)
                        {
                            if( !Character.isDigit(sv.charAt(i)) ) {
                                throw new AlgebricksException("Invalid number. Expected argument is a number from 0 "
                                        + "to INF, where INF is infinity");
                            }
                        }
                        inputNumber = Long.parseLong(sv);
                    }
            }
        } else {
            throw new AlgebricksException("Invalid argument. Expected a number or INF (where INF is infinity), but got "
                    + arg1Expr);
        }

        if (inputNumber == 0) { // We want to print out an array of bytes
            return new AOrderedListType(BuiltinType.AINT16, null);
        } else {
            IAType arg0Type = (IAType) env.getType(f.getArguments().get(0).getValue());
            // Input type argument that can be simple or complex object
            switch (arg0Type.getTypeTag()) {
                case RECORD:
                    return DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
                case UNORDEREDLIST:
                case ORDEREDLIST:
                    return DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE;
                default:
                    return fieldRecordType;
            }
        }
    }

    private long getNumberValue (ConstantExpression expr) throws AlgebricksException {
        IAObject aInteger = ((AsterixConstantValue) expr.getValue()).getObject();
        ATypeTag tt = aInteger.getType().getTypeTag();
        switch (tt) {
            case INT16:
                return ((AInt16)aInteger).getShortValue();
            case INT32:
                return ((AInt32)aInteger).getIntegerValue();
            case INT64:
                return ((AInt64)aInteger).getLongValue();
            default:
                throw new AlgebricksException("Invalid number. Expected argument is a number from 0 "
                        + "to INF, where INF is infinity");
        }
    }

}
