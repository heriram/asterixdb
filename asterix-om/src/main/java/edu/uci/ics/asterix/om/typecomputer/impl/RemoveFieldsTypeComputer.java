package edu.uci.ics.asterix.om.typecomputer.impl;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.base.AOrderedList;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.types.*;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import org.apache.commons.lang3.mutable.Mutable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Cases to support:
 * remove-fields($record, ["foo", ["bar", "access"]]),
 * where ["bar", "access"] is equivalent to the path bar->access
 *
 */
public class RemoveFieldsTypeComputer extends AbstractRecordManipulationTypeComputer {

    private static final long serialVersionUID = 1L;

    public static final RemoveFieldsTypeComputer INSTANCE = new RemoveFieldsTypeComputer();

    private final List<List<String>> pathList = new ArrayList<List<String>>();

    private final PathComparator pathComparator = PathComparator.INSTANCE;

    private RemoveFieldsTypeComputer() {
    }

    private ARecordType inputRecordType;


    private void getPathFromConstantExpression(ILogicalExpression expression, List<String> path) throws AlgebricksException {
        ConstantExpression ce = (ConstantExpression) expression;
        if (!(ce.getValue() instanceof AsterixConstantValue)) {
            throw new AlgebricksException("Expecting a list of strings and found " + ce.getValue() + " instead.");
        }
        IAObject item = ((AsterixConstantValue) ce.getValue()).getObject();
        ATypeTag type = item.getType().getTypeTag();

        switch (type) {
            case STRING:
                path.add(((AString) item).getStringValue());
                break;
            case ORDEREDLIST:
                AOrderedList pathOrdereList = (AOrderedList) item;
                for (int i = 0; i < pathOrdereList.size(); i++) {
                    path.add(((AString) pathOrdereList.getItem(i)).getStringValue());
                }
                break;
            default:
                throw new AlgebricksException("Unsupport type: " + type);
        }
    }

    private void handleConstantExpression(ILogicalExpression expression)
            throws AlgebricksException {
        List<String> path = getStringListFromPool();
        getPathFromConstantExpression(expression, path);
        pathList.add(path);
    }

    private void getPathFromFunction(ILogicalExpression expression, List<String> path)
            throws AlgebricksException {
        AbstractFunctionCallExpression funcExp = (AbstractFunctionCallExpression) expression;
        List<Mutable<ILogicalExpression>> args = funcExp.getArguments();

        for (int i=0; i<args.size(); i++) { // Mutable<ILogicalExpression> arg : argList) {
            // At this point all elements has to be a constant
            ConstantExpression  ce = (ConstantExpression) args.get(i).getValue();
            if (!(ce.getValue() instanceof AsterixConstantValue)) {
                throw new AlgebricksException("Expecting a list of strings and found " + ce.getValue() + " instead.");
            }
            getPathFromConstantExpression(ce, path);
        }
    }


    private void handleNonConstantExpression(ILogicalExpression expression)
            throws AlgebricksException {
        AbstractFunctionCallExpression funcExp = (AbstractFunctionCallExpression) expression;
        List<Mutable<ILogicalExpression>> argList = funcExp.getArguments();

        for (Mutable<ILogicalExpression> arg : argList) {
            ILogicalExpression argExp = arg.getValue();
            List<String> path = getStringListFromPool();
            switch (argExp.getExpressionTag()) {
                case CONSTANT:
                    getPathFromConstantExpression(argExp, path);
                    break;
                case FUNCTION_CALL:
                    getPathFromFunction(argExp, path);
                    break;
                default:
                    throw new AlgebricksException("Unsupported expression: " + argExp);

            }
            pathList.add(path);
        }


    }

    @SuppressWarnings("unchecked")
    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {

        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;
        IAType type0 = (IAType) env.getType(f.getArguments().get(0).getValue());

        inputRecordType = NonTaggedFieldAccessByNameResultType.getRecordTypeFromType(type0, expression);
        if (inputRecordType == null) {
            return BuiltinType.ANY;
        }

        AbstractLogicalExpression arg1 = (AbstractLogicalExpression) f.getArguments().get(1).getValue();
        IAType inputListType = (IAType) env.getType(arg1);
        AOrderedListType inputOrderedListType = extractOrderedListType(inputListType);
        if (inputOrderedListType == null) {
            throw new AlgebricksException(
                    "The function 'remove-fields' expects an ordered list as the second argument, but got "
                            + inputListType);
        }

        pathList.clear();

        IAType outputRecordType = null;
        if (arg1.getExpressionTag() == LogicalExpressionTag.CONSTANT) { // Constant list
            handleConstantExpression(arg1);
        } else if (arg1.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) { // Nested (path) type
            AbstractFunctionCallExpression funcExp = (AbstractFunctionCallExpression) arg1;
            handleNonConstantExpression(funcExp);
        }

        outputRecordType = buildOutputType(inputRecordType);
        return outputRecordType;
    }

    private IAType buildOutputType(ARecordType inputRecordType)
            throws AlgebricksException {
        IAType resultType = null;
        List<String> resultFieldNames = getStringListFromPool();
        List<IAType> resultFieldTypes = getATypeListFromPool();

        for (String fieldName : inputRecordType.getFieldNames()) {
            try {
                if (!contains(fieldName)) {
                    resultFieldNames.add(fieldName);
                    if (inputRecordType.getFieldType(fieldName).getTypeTag() == ATypeTag.RECORD) {
                        ARecordType nestedType = (ARecordType) inputRecordType.getFieldType(fieldName);
                        //Deep Copy prevents altering of input types
                        resultFieldTypes.add(deepCopy(Arrays.asList(new String[] { fieldName }), nestedType));
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

        returnStringListToPool(resultFieldNames);
        returnATypeListToPool(resultFieldTypes);

        return resultType;
    }

    private boolean contains(String fieldName) {
        List<String> path = getStringListFromPool();
        path.add(fieldName);
        boolean found =  contains(path);
        returnStringListToPool(path);
        return found;
    }

    private boolean contains(List<String> path) {
        for (int i = 0; i < pathList.size(); i++) {
            if (pathComparator.compare(path, pathList.get(i))) {
                return true;
            }
        }
        return false;
    }


    public ARecordType deepCopy(List<String> parentPath, ARecordType sourceRecordType) {
        String srcFieldNames[] = sourceRecordType.getFieldNames();
        IAType srcFieldTypes[] = sourceRecordType.getFieldTypes();

        IAType[] tempTypes = new IAType[srcFieldTypes.length];
        String[] tempNames = new String[srcFieldTypes.length];
        int fieldCount = 0;

        List<String> path = getStringListFromPool();
        path.addAll(parentPath);
        int pathLength = path.size();
        for (int i = 0; i < srcFieldNames.length; i++) {
            path.add(srcFieldNames[i]);
            if (!contains(path)) {
                tempNames[fieldCount] = srcFieldNames[i];
                if (srcFieldTypes[i].getTypeTag() == ATypeTag.RECORD) {
                    tempTypes[fieldCount] = deepCopy(path, (ARecordType) srcFieldTypes[i]);
                } else {
                    tempTypes[fieldCount] = srcFieldTypes[i];

                }
                fieldCount++;

            }
            path.remove(pathLength);
        }
        returnStringListToPool(path);

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