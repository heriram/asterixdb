package edu.uci.ics.asterix.om.typecomputer.impl;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
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
                    if (fieldType1Copy.getFieldTypes()[i].getTypeTag() != ATypeTag.RECORD) {
                        throw new AlgebricksException("Duplicate field " + fieldType1Copy.getTypeName() + " encountered");
                        // TODO For next version: check equality of the content at runtime first before throwing an error
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

    public void reset() {
        stringListPool.clear();
        aTypelistPool.clear();
    }
    
    abstract public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException;

}
