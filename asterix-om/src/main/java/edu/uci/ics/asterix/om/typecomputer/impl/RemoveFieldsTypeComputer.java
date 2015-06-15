package edu.uci.ics.asterix.om.typecomputer.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.base.AOrderedList;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class RemoveFieldsTypeComputer extends AbstractRecordManipulationTypeComputer {

    private static final long serialVersionUID = 1L;

    public static final RemoveFieldsTypeComputer INSTANCE = new RemoveFieldsTypeComputer();

    private RemoveFieldsTypeComputer() {
    }

    @SuppressWarnings("unchecked")
    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {

        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;
        IAType inputRecType = (IAType) env.getType(f.getArguments().get(0).getValue());
        ILogicalExpression secondArgExpression = f.getArguments().get(1).getValue();
        IAType inputListType = (IAType) env.getType(secondArgExpression);
        ARecordType inputRecordType = extractRecordType(inputRecType);
        AOrderedListType inputOrderedListType = extractOrderedListType(inputListType);

        if (inputRecordType == null) {
            throw new AlgebricksException(
                    "The function 'remove-fields' expects a record as the first argument, but got " + inputRecType);
        }

        if (inputOrderedListType == null) {
            throw new AlgebricksException(
                    "The function 'remove-fields' expects an ordered list as the second argument, but got "
                            + inputListType);
        }

        List<String> removePaths[] = null;

        // If list constructor function get the list from that function first
        if (secondArgExpression.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression funcExp = (AbstractFunctionCallExpression) secondArgExpression;
            List<Mutable<ILogicalExpression>> argList = funcExp.getArguments();
            removePaths = new List[argList.size()];
            for (int i = 0; i < argList.size(); i++) {
                ConstantExpression listElementExpr = (ConstantExpression) argList.get(i).getValue();
                if (!(listElementExpr.getValue() instanceof AsterixConstantValue)) {
                    throw new AlgebricksException("Expecting a list of strings and found " + listElementExpr.getValue()
                            + " instead.");
                }
                AString s = (AString) ((AsterixConstantValue) listElementExpr.getValue()).getObject();
                removePaths[i] = Arrays.asList(StringUtils.split(s.getStringValue(), '.'));
            }

        } else if (secondArgExpression.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            ConstantExpression listExpr = (ConstantExpression) secondArgExpression;
            if (!(listExpr.getValue() instanceof AsterixConstantValue)) {
                throw new AlgebricksException("Expecting a list of strings and found " + listExpr.getValue()
                        + " instead.");
            }
            AOrderedList removeList = (AOrderedList) ((AsterixConstantValue) listExpr.getValue()).getObject();
            removePaths = new List[removeList.size()];
            for (int i = 0; i < removeList.size(); i++) {
                AString s = (AString) removeList.getItem(i);
                removePaths[i] = Arrays.asList(StringUtils.split(s.getStringValue(), '.'));
            }

        } else {
            throw new AlgebricksException("Unsupported expression form: " + secondArgExpression.getExpressionTag());
        }
        
        IAType outputRecordType = buildOutputType(inputRecordType, removePaths);

        return outputRecordType;
    }

    private IAType buildOutputType(ARecordType inputRecordType, List<String> removePaths[]) throws AlgebricksException {
        IAType resultType = null;
        List<String> resultFieldNames = new ArrayList<>();
        List<IAType> resultFieldTypes = new ArrayList<>();

        for (String fieldName : inputRecordType.getFieldNames()) {
            try {
                if (!contains(removePaths, fieldName)) {
                    resultFieldNames.add(fieldName);
                    if (inputRecordType.getFieldType(fieldName).getTypeTag() == ATypeTag.RECORD) {
                        ARecordType nestedType = (ARecordType) inputRecordType.getFieldType(fieldName);
                        //Deep Copy prevents altering of input types
                        resultFieldTypes.add(deepCopy(Arrays.asList(new String[] { fieldName }), nestedType,
                                removePaths));
                    } else {
                        resultFieldTypes.add(inputRecordType.getFieldType(fieldName));
                    }
                }
            } catch (IOException e) {
                throw new AlgebricksException(e);
            }
        }

        int n = resultFieldNames.size();
        String resultTypeName = "result-record(" + inputRecordType.getTypeName() + ")";
        try {
            resultType = new ARecordType(resultTypeName, resultFieldNames.toArray(new String[n]),
                    resultFieldTypes.toArray(new IAType[n]), inputRecordType.isOpen());
        } catch (HyracksDataException | AsterixException e) {
            throw new AlgebricksException(e);
        }

        return resultType;
    }
    private boolean contains(List<String> removePaths[], String fieldName) {
        return contains(removePaths, Arrays.asList(new String[] { fieldName }));
    }

    private boolean contains(List<String> removePaths[], List<String> path) {
        if (path == null)
            return false;

        for (int i = 0; i < removePaths.length; i++) {
            List<String> p = removePaths[i];
            if (p.size() == path.size()) {
                boolean found = true;
                for (int j = 0; j < p.size(); j++) {
                    found &= p.get(j).equals(path.get(j));
                }
                if (found)
                    return true;
            }
        }

        return false;
    }

    public ARecordType deepCopy(List<String> parentPath, ARecordType sourceRecordType, List<String> removePaths[]) {
        String srcFieldNames[] = sourceRecordType.getFieldNames();
        IAType srcFieldTypes[] = sourceRecordType.getFieldTypes();

        IAType[] tempTypes = new IAType[srcFieldTypes.length];
        String[] tempNames = new String[srcFieldTypes.length];
        int fieldCount = 0;

        List<String> path = new ArrayList<String>();
        path.addAll(parentPath);
        int pathLength = path.size();
        for (int i = 0; i < srcFieldNames.length; i++) {
            path.add(srcFieldNames[i]);
            if (!contains(removePaths, path)) {
                tempNames[fieldCount] = srcFieldNames[i];
                if (srcFieldTypes[i].getTypeTag() == ATypeTag.RECORD) {
                    tempTypes[fieldCount] = deepCopy(path, (ARecordType) srcFieldTypes[i], removePaths);
                } else {
                    tempTypes[fieldCount] = srcFieldTypes[i];

                }
                fieldCount++;

            }
            path.remove(pathLength);
        }

        IAType[] newTypes = new IAType[fieldCount];
        String[] newNames = new String[fieldCount];

        System.arraycopy(tempNames, 0, newNames, 0, fieldCount);
        System.arraycopy(tempTypes, 0, newTypes, 0, fieldCount);

        ARecordType destRecordType = null;

        try {
            destRecordType = new ARecordType(sourceRecordType.getTypeName(), newNames, newTypes,
                    sourceRecordType.isOpen());
        } catch (HyracksDataException | AsterixException e) {
            e.printStackTrace();
            new AlgebricksException(e);
        }
        return destRecordType;
    }
}