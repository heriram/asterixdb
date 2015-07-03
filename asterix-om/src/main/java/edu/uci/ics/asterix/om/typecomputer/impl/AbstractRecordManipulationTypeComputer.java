package edu.uci.ics.asterix.om.typecomputer.impl;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.types.*;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.util.*;

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

    protected static class PathComparator  {
        public static final PathComparator INSTANCE = new PathComparator();

        private PathComparator() {

        }

        public boolean compare(List<String> path1, List<String> path2) {
            if (path1==null || path2==null) {
                return false;
            } else if(path1.size() != path2.size()) {
                return false;
            }
            boolean found = true;
            for (int j = 0; j < path1.size(); j++) {
                found &= path1.get(j).equals(path2.get(j));
            }

            return found;

        }
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
