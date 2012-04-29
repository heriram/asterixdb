package edu.uci.ics.asterix.algebra.operators.physical;

import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.functions.FunctionArgumentsConstants;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.declared.AqlSourceId;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.optimizer.rules.am.BTreeJobGenParams;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.api.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;

public class BTreeSearchPOperator extends IndexSearchPOperator {

    public BTreeSearchPOperator(IDataSourceIndex<String, AqlSourceId> idx, boolean requiresBroadcast) {
        super(idx, requiresBroadcast);
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.BTREE_SEARCH;
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
                    throws AlgebricksException {
        UnnestMapOperator unnestMap = (UnnestMapOperator) op;
        ILogicalExpression unnestExpr = unnestMap.getExpressionRef().getValue();
        if (unnestExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            throw new IllegalStateException();
        }
        AbstractFunctionCallExpression unnestFuncExpr = (AbstractFunctionCallExpression) unnestExpr;
        FunctionIdentifier funcIdent = unnestFuncExpr.getFunctionIdentifier();
        if (!funcIdent.equals(AsterixBuiltinFunctions.INDEX_SEARCH)) {            
            return;
        }
        BTreeJobGenParams jobGenParams = new BTreeJobGenParams();
        jobGenParams.readFromFuncArgs(unnestFuncExpr.getArguments());
        if (!jobGenParams.getIndexType().equals(FunctionArgumentsConstants.BTREE_INDEX)) {
            throw new NotImplementedException(jobGenParams.getIndexType() + " indexes are not implemented.");
        }
        int[] lowKeyIndexes = getKeyIndexes(jobGenParams.getLowKeyVarList(), inputSchemas);
        int[] highKeyIndexes = getKeyIndexes(jobGenParams.getHighKeyVarList(), inputSchemas);
        AqlMetadataProvider metadataProvider = (AqlMetadataProvider) context.getMetadataProvider();
        AqlCompiledMetadataDeclarations metadata = metadataProvider.getMetadataDeclarations();
        AqlCompiledDatasetDecl datasetDecl = metadata.findDataset(jobGenParams.getDatasetName());
        if (datasetDecl == null) {
            throw new AlgebricksException("Unknown dataset " + jobGenParams.getDatasetName());
        }
        if (datasetDecl.getDatasetType() == DatasetType.EXTERNAL) {
            throw new AlgebricksException("Trying to run btree search over external dataset ("
                    + jobGenParams.getDatasetName() + ").");
        }
        IVariableTypeEnvironment typeEnv = context.getTypeEnvironment(unnestMap);
        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> btreeSearch = AqlMetadataProvider.buildBtreeRuntime(
                metadata, context, builder.getJobSpec(), typeEnv, inputSchemas, jobGenParams.getRetainInput(),
                jobGenParams.getDatasetName(), datasetDecl, jobGenParams.getIndexName(), lowKeyIndexes, highKeyIndexes,
                jobGenParams.isLowKeyInclusive(), jobGenParams.isHighKeyInclusive());
        builder.contributeHyracksOperator(unnestMap, btreeSearch.first);
        builder.contributeAlgebricksPartitionConstraint(btreeSearch.first, btreeSearch.second);

        ILogicalOperator srcExchange = unnestMap.getInputs().get(0).getValue();
        builder.contributeGraphEdge(srcExchange, 0, unnestMap, 0);
    }
}