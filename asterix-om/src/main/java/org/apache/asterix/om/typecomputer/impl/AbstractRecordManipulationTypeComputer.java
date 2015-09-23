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

import  org.apache.asterix.common.exceptions.AsterixException;
import  org.apache.asterix.om.base.AString;
import  org.apache.asterix.om.base.IAObject;
import  org.apache.asterix.om.constants.AsterixConstantValue;
import  org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import  org.apache.asterix.om.types.AOrderedListType;
import  org.apache.asterix.om.types.ARecordType;
import  org.apache.asterix.om.types.ATypeTag;
import  org.apache.asterix.om.types.AUnionType;
import  org.apache.asterix.om.types.AUnorderedListType;
import  org.apache.asterix.om.types.IAType;
import  org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import  org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import  org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import  org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import  org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import  org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.mutable.Mutable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

abstract public class AbstractRecordManipulationTypeComputer implements IResultTypeComputer {
    
    private static final long serialVersionUID = 1L;

    private final Deque<List<String>> stringListPool = new ArrayDeque<>();
    private final Deque<List<IAType>> aTypelistPool = new ArrayDeque<>();


    protected List<String> getStringListFromPool() {
        List<String> list = stringListPool.poll();
        if (list != null) {
            return list;
        } else {
            return new ArrayList<>();
        }
    }

    protected List<IAType> getATypeListFromPool() {
        List<IAType> list = aTypelistPool.poll();
        if (list != null) {
            return list;
        } else {
            return new ArrayList<>();
        }
    }

    protected void returnStringListToPool(List<String> list) {
        if(!list.isEmpty()) {
            list.clear();
        }
        stringListPool.add(list);
    }

    protected void returnATypeListToPool(List<IAType> list) {
        if(!list.isEmpty()) {
            list.clear();
        }
        aTypelistPool.add(list);
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
    
    public static AOrderedListType extractOrderedListType(IAType t) {
        if (t.getTypeTag() == ATypeTag.ORDEREDLIST) {
            return (AOrderedListType) t;
        }

        if (t.getTypeTag() == ATypeTag.UNION) {
            IAType innerType = ((AUnionType) t).getUnionList().get(1);
            if (innerType.getTypeTag() == ATypeTag.ORDEREDLIST) {
                return (AOrderedListType) innerType;
            }
        }

        return null;
    }
    
    public static AUnorderedListType extractUnorderedListType(IAType t) {
        if (t.getTypeTag() == ATypeTag.UNORDEREDLIST) {
            return (AUnorderedListType) t;
        }

        if (t.getTypeTag() == ATypeTag.UNION) {
            IAType innerType = ((AUnionType) t).getUnionList().get(1);
            if (innerType.getTypeTag() == ATypeTag.UNORDEREDLIST) {
                return (AUnorderedListType) innerType;
            }
        }

        return null;
    }

    protected void addAdditionalFields(List<String> resultFieldNames, List<IAType> resultFieldTypes,
            List<String> additionalFieldNames , List<IAType> additionalFieldTypes) throws AlgebricksException {

        for (int i=0; i<additionalFieldNames.size(); i++) {
            String fn = additionalFieldNames.get(i);
            IAType ft = additionalFieldTypes.get(i);
            int pos = Collections.binarySearch(resultFieldNames, fn);
            if (pos >= 0) {
                IAType rt = resultFieldTypes.get(pos);
                if (rt.getTypeTag() != ft.getTypeTag()) {
                    throw new AlgebricksException("Duplicate field " + fn + " encountered");
                }
                try {
                    // Only need to merge records
                    if (ft.getTypeTag() == ATypeTag.RECORD && rt.getTypeTag() == ATypeTag.RECORD) {
                        resultFieldTypes.set(pos, mergedNestedType(ft, rt));
                    }
                } catch (AsterixException e) {
                    throw new AlgebricksException(e);
                }

            } else {
                resultFieldNames.add(fn);
                resultFieldTypes.add(ft);
            }
        }
    }

    protected List<String> getListFromExpression(ILogicalExpression expression)
            throws AlgebricksException {
        AbstractFunctionCallExpression funcExp = (AbstractFunctionCallExpression) expression;
        List<Mutable<ILogicalExpression>> args = funcExp.getArguments();

        List<String> list = getStringListFromPool();
        for (Mutable<ILogicalExpression> arg : args) {
            // At this point all elements has to be a constant
            // Input list has only one level of nesting (list of list or list of strings)
            ConstantExpression ce = (ConstantExpression) arg.getValue();
            if (!(ce.getValue() instanceof AsterixConstantValue)) {
                throw new AlgebricksException("Expecting a list of strings and found " + ce.getValue() + " instead.");
            }
            IAObject item = ((AsterixConstantValue) ce.getValue()).getObject();
            ATypeTag type = item.getType().getTypeTag();
            if (type == ATypeTag.STRING) {
                list.add(((AString) item).getStringValue());
            } else {
                throw new AlgebricksException(type + " is currently not supported. Please check your function call.");
            }
        }

        return list;
    }

    public static IAType mergedNestedType(IAType fieldType1, IAType fieldType0) throws AlgebricksException,
            AsterixException {
        if (fieldType1.getTypeTag() != ATypeTag.RECORD || fieldType0.getTypeTag() != ATypeTag.RECORD) {
            throw new AlgebricksException("Duplicate field " + fieldType1.getTypeName() + " encountered");
        }

        ARecordType returnType = (ARecordType) fieldType0;
        ARecordType fieldType1Copy = (ARecordType) fieldType1;

        for (int i = 0; i < fieldType1Copy.getFieldTypes().length; i++) {
            try {
                int pos = returnType.findFieldPosition(fieldType1Copy.getFieldNames()[i]);
                if (pos >= 0) {
                    // If a sub-record do merge, else ignore and let the values decide what to do
                    if (fieldType1Copy.getFieldTypes()[i].getTypeTag() == ATypeTag.RECORD) {
                        IAType[] oldTypes = returnType.getFieldTypes();
                        oldTypes[pos] = mergedNestedType(fieldType1Copy.getFieldTypes()[i], returnType.getFieldTypes()[pos]);
                        returnType = new ARecordType(returnType.getTypeName(), returnType.getFieldNames(), oldTypes,
                                returnType.isOpen());
                    }
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

    public void reset() {
        stringListPool.clear();
        aTypelistPool.clear();
    }
    
    abstract public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException;

}
