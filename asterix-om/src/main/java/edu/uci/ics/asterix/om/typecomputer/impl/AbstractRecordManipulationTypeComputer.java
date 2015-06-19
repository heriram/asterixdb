package edu.uci.ics.asterix.om.typecomputer.impl;

import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.types.*;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

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

    public void reset() {
        stringListPool.clear();
        aTypelistPool.clear();
    }
    
    abstract public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException;

}
