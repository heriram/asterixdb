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
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.*;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.commons.lang3.mutable.Mutable;

import java.io.IOException;
import java.util.*;

/**
 * Cases to support:
 * remove-fields($record, ["foo", ["bar", "access"]]),
 * where ["bar", "access"] is equivalent to the path bar->access
 *
 */
public class RecordRemoveFieldsTypeComputer extends AbstractRecordManipulationTypeComputer {

    private static final long serialVersionUID = 1L;

    public static final RecordRemoveFieldsTypeComputer INSTANCE = new RecordRemoveFieldsTypeComputer();

    // A set of fieldnames for faster checking purposes
    private final Set<String> fieldNameSet = new HashSet<>();

    // A list of list holding the list of paths function input arg. #2
    private final List<List<String>> pathList = new ArrayList<>();

    // A stack used for processing nested recordtypes
    private final Deque<String> fieldPathStack = new ArrayDeque<>();

    private RecordRemoveFieldsTypeComputer() {
    }

    private ARecordType inputRecordType;


    private void getPathFromConstantExpression(ILogicalExpression expression) throws AlgebricksException {
        ConstantExpression ce = (ConstantExpression) expression;
        if (!(ce.getValue() instanceof AsterixConstantValue)) {
            throw new AlgebricksException("Expecting a list of strings and found " + ce.getValue() + " instead.");
        }
        IAObject item = ((AsterixConstantValue) ce.getValue()).getObject();
        ATypeTag type = item.getType().getTypeTag();

        switch (type) {
            case STRING:
                String fn = ((AString) item).getStringValue();
                fieldNameSet.add(fn);
                break;
            case ORDEREDLIST:
                AOrderedList pathOrdereList = (AOrderedList) item;
                String fieldName = ((AString) pathOrdereList.getItem(0)).getStringValue();
                fieldNameSet.add(fieldName);
                List<String> path = getStringListFromPool();
                for (int i = 0; i < pathOrdereList.size(); i++) {
                    path.add(((AString) pathOrdereList.getItem(i)).getStringValue());
                }
                pathList.add(path);
                break;
            default:
                throw new AlgebricksException("Unsupport type: " + type);
        }
    }


    private void getPathFromFunctionExpression(ILogicalExpression expression)
            throws AlgebricksException {
        List<String> path = getListFromExpression(expression);
        // Add the path head to remove set
        fieldNameSet.add(path.get(0));
        pathList.add(path);

    }


    private void computeTypeFromNonConstantExpression(ILogicalExpression expression)
            throws AlgebricksException {
        AbstractFunctionCallExpression funcExp = (AbstractFunctionCallExpression) expression;
        List<Mutable<ILogicalExpression>> args = funcExp.getArguments();

        for (Mutable<ILogicalExpression> arg : args) {
            ILogicalExpression le = arg.getValue();
            switch (le.getExpressionTag()) {
                case CONSTANT:
                    getPathFromConstantExpression(le);
                    break;
                case FUNCTION_CALL:
                    getPathFromFunctionExpression(le);
                    break;
                default:
                    throw new AlgebricksException("Unsupported expression: " + le);

            }
        }
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expression;
        IAType type0 = (IAType) env.getType(funcExpr.getArguments().get(0).getValue());

        inputRecordType = NonTaggedFieldAccessByNameResultType.getRecordTypeFromType(type0, expression);
        if (inputRecordType == null) {
            return BuiltinType.ANY;
        }

        AbstractLogicalExpression arg1 = (AbstractLogicalExpression) funcExpr.getArguments().get(1).getValue();
        IAType inputListType = (IAType) env.getType(arg1);
        AOrderedListType inputOrderedListType = extractOrderedListType(inputListType);
        if (inputOrderedListType == null) {
            throw new AlgebricksException(
                    "The function 'remove-fields' expects an ordered list as the second argument, but got "
                            + inputListType);
        }

        ATypeTag tt = inputOrderedListType.getItemType().getTypeTag();
        fieldNameSet.clear();
        pathList.clear();
        if (tt == ATypeTag.STRING) {  // If top-fieldlist
            if (setFieldNameSet(arg1)) {
                return buildOutputType(inputRecordType);
            } else {
                return DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
            }
        } else { // tt == ATypeTag.ANY, meaning the list is nested
            computeTypeFromNonConstantExpression(arg1);
            IAType resultType = buildOutputType(inputRecordType);
            pathList.clear();
            return resultType;
        }
    }

    private boolean setFieldNameSet(ILogicalExpression expr) {
        if (expr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            AOrderedList orderedList = (AOrderedList) (((AsterixConstantValue) ((ConstantExpression) expr).getValue())
                    .getObject());
            for (int i = 0; i < orderedList.size(); i++) {
                AString as = (AString) orderedList.getItem(i);
                fieldNameSet.add(as.getStringValue());
            }
            return true; // Success
        }
        return false;
    }

    private void addField(List<String> resultFieldNames, List<IAType> resultFieldTypes, String fieldName)
            throws AlgebricksException {
        try {
            resultFieldNames.add(fieldName);
            if (inputRecordType.getFieldType(fieldName).getTypeTag() == ATypeTag.RECORD) {
                ARecordType nestedType = (ARecordType) inputRecordType.getFieldType(fieldName);
                //Deep Copy prevents altering of input types
                resultFieldTypes.add(nestedType.deepCopy(nestedType));
            } else {
                resultFieldTypes.add(inputRecordType.getFieldType(fieldName));
            }

        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

    private IAType buildOutputType(ARecordType inputRecordType)
            throws AlgebricksException {
        IAType resultType;
        List<String> resultFieldNames = getStringListFromPool();
        List<IAType> resultFieldTypes = getATypeListFromPool();

        String[] fieldNames = inputRecordType.getFieldNames();
        IAType[] fieldTypes = inputRecordType.getFieldTypes();

        fieldPathStack.clear();
        for (int i=0; i < fieldNames.length; i++) {
            if (!fieldNameSet.contains(fieldNames[i])) { // The main field is to be kept
                addField(resultFieldNames, resultFieldTypes, fieldNames[i]);
            } else if(!pathList.isEmpty()){ // Further check needed for nested fields
                if (fieldTypes[i].getTypeTag() == ATypeTag.RECORD) {
                    ARecordType subRecord = (ARecordType)fieldTypes[i];

                    fieldPathStack.push(fieldNames[i]);
                    subRecord = deepCheckAndCopy(fieldPathStack, subRecord);
                    fieldPathStack.pop();
                    if (subRecord != null) {
                        resultFieldNames.add(fieldNames[i]);
                        resultFieldTypes.add(subRecord);
                    }
                }
            }
        }

        int n = resultFieldNames.size();
        String resultTypeName = "result-record(" + inputRecordType.getTypeName() + ")";
        try {
            resultType = new ARecordType(resultTypeName, resultFieldNames.toArray(new String[n]),
                    resultFieldTypes.toArray(new IAType[n]), true); // Make the output type open always
        } catch (HyracksDataException | AsterixException e) {
            throw new AlgebricksException(e);
        }

        returnStringListToPool(resultFieldNames);
        returnATypeListToPool(resultFieldTypes);

        return resultType;
    }

    /**
     * Comparison elements of two paths
     *
     *  Note: l2 uses a LIFO insert and removal.
     */
    private <E> boolean isEqualPaths(List<E> l1, Deque<E> l2) {
        if ((l1 == null) || (l2 == null)) return false;

        if (l1.size() != l2.size()) return false;

        Iterator<E> it2 = l2.iterator();

        int len = l1.size();
        for (int i=len-1; i>=0; i--) {
            E o1 = l1.get(i);
            E o2 = it2.next();
            if(!o1.equals(o2)) return false;
        }
        return true;
    }


    private boolean isRemovePath(Deque<String> fieldPath) {
        for (List<String> removePath: pathList) {
            if (isEqualPaths(removePath, fieldPath)) {
                return true;
            }
        }
        return false;
    }

    /*
        A method to deep copy a record the path validation
             i.e., keep only fields that are valid
     */
    private ARecordType deepCheckAndCopy(Deque<String> fieldPath, ARecordType srcRecType) throws AlgebricksException {
        // Make sure the current path is valid before going further
        if (isRemovePath(fieldPath)) {
            return null;
        }

        String srcFieldNames[] = srcRecType.getFieldNames();
        IAType srcFieldTypes[] = srcRecType.getFieldTypes();

        // Allocate memory
        List<IAType> destFieldTypes = getATypeListFromPool();
        List<String> destFieldNames = getStringListFromPool();

        for (int i = 0; i < srcFieldNames.length; i++) {
            fieldPath.push(srcFieldNames[i]);
            if (!isRemovePath(fieldPath)) {
                if (srcFieldTypes[i].getTypeTag() == ATypeTag.RECORD) {
                    ARecordType subRecord = (ARecordType) srcFieldTypes[i];
                    subRecord = deepCheckAndCopy(fieldPath, subRecord);
                    if (subRecord != null) {
                        destFieldNames.add(srcFieldNames[i]);
                        destFieldTypes.add(subRecord);
                    }
                } else {
                    destFieldNames.add(srcFieldNames[i]);
                    destFieldTypes.add(srcFieldTypes[i]);
                }
            }
            fieldPath.pop();
        }

        ARecordType destRecordType;
        int n = destFieldNames.size();

        if(n==0) {
            // Free memory
            returnStringListToPool(destFieldNames);
            returnATypeListToPool(destFieldTypes);

            return null;
        }

        try {
            destRecordType = new ARecordType(srcRecType.getTypeName(), destFieldNames.toArray(new String[n]),
                    destFieldTypes.toArray(new IAType[n]), inputRecordType.isOpen());
        } catch (HyracksDataException | AsterixException e) {
            throw new AlgebricksException(e);
        }
        // Free memory
        returnStringListToPool(destFieldNames);
        returnATypeListToPool(destFieldTypes);
        return destRecordType;
    }

}