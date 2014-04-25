/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.metadata.feeds;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.Pair;

import edu.uci.ics.asterix.common.dataflow.AsterixLSMInvertedIndexInsertDeleteOperatorDescriptor;
import edu.uci.ics.asterix.common.dataflow.AsterixLSMTreeInsertDeleteOperatorDescriptor;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedPolicyAccessor;
import edu.uci.ics.asterix.common.feeds.FeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataConstants;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.entities.DatasourceAdapter;
import edu.uci.ics.asterix.metadata.entities.DatasourceAdapter.AdapterType;
import edu.uci.ics.asterix.metadata.entities.Datatype;
import edu.uci.ics.asterix.metadata.entities.Feed;
import edu.uci.ics.asterix.metadata.entities.Function;
import edu.uci.ics.asterix.metadata.entities.PrimaryFeed;
import edu.uci.ics.asterix.metadata.entities.SecondaryFeed;
import edu.uci.ics.asterix.metadata.functions.ExternalLibraryManager;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import edu.uci.ics.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.std.StreamProjectRuntimeFactory;
import edu.uci.ics.hyracks.api.constraints.Constraint;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstantExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstraintExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.LValueConstraintExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionCountExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionLocationExpression;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.job.JobSpecification;

/**
 * A utility class for providing helper functions for feeds
 */
public class FeedUtil {

    private static Logger LOGGER = Logger.getLogger(FeedUtil.class.getName());

    public static String getFeedPointKeyRep(Feed feed, List<String> appliedFunctions) {
        StringBuilder builder = new StringBuilder();
        builder.append(feed.getDataverseName() + ":");
        builder.append(feed.getFeedName() + ":");
        if (appliedFunctions != null && !appliedFunctions.isEmpty()) {
            for (String function : appliedFunctions) {
                builder.append(function + ":");
            }
            builder.deleteCharAt(builder.length() - 1);
        }
        return builder.toString();
    }

    private static class LocationConstraint {
        int partition;
        String location;
    }

    public static JobSpecification alterJobSpecificationForFeed(JobSpecification spec,
            FeedConnectionId feedConnectionId, Map<String, String> feedPolicyProperties) {

        FeedPolicyAccessor fpa = new FeedPolicyAccessor(feedPolicyProperties);

        JobSpecification altered = new JobSpecification(spec.getFrameSize());
        Map<OperatorDescriptorId, IOperatorDescriptor> operatorMap = spec.getOperatorMap();

        boolean preProcessingRequired = preProcessingRequired(feedConnectionId);
        // copy operators
        String operandId = null;
        Map<OperatorDescriptorId, OperatorDescriptorId> oldNewOID = new HashMap<OperatorDescriptorId, OperatorDescriptorId>();
        FeedMetaOperatorDescriptor metaOp = null;
        for (Entry<OperatorDescriptorId, IOperatorDescriptor> entry : operatorMap.entrySet()) {
            operandId = FeedRuntimeId.DEFAULT_OPERAND_ID;
            IOperatorDescriptor opDesc = entry.getValue();
            if (opDesc instanceof FeedCollectOperatorDescriptor) {
                FeedCollectOperatorDescriptor orig = (FeedCollectOperatorDescriptor) opDesc;
                FeedCollectOperatorDescriptor fiop = new FeedCollectOperatorDescriptor(altered,
                        orig.getFeedConnectionId(), orig.getSourceFeedId(), (ARecordType) orig.getOutputType(),
                        orig.getRecordDescriptor(), orig.getFeedPolicyProperties(), orig.getSubscriptionLocation());
                oldNewOID.put(opDesc.getOperatorId(), fiop.getOperatorId());
            } else if (opDesc instanceof AsterixLSMTreeInsertDeleteOperatorDescriptor) {
                operandId = ((AsterixLSMTreeInsertDeleteOperatorDescriptor) opDesc).getIndexName();
                metaOp = new FeedMetaOperatorDescriptor(altered, feedConnectionId, opDesc, feedPolicyProperties,
                        FeedRuntimeType.STORE, false, operandId);
                oldNewOID.put(opDesc.getOperatorId(), metaOp.getOperatorId());
            } else if (opDesc instanceof AsterixLSMInvertedIndexInsertDeleteOperatorDescriptor) {
                operandId = ((AsterixLSMInvertedIndexInsertDeleteOperatorDescriptor) opDesc).getIndexName();
                metaOp = new FeedMetaOperatorDescriptor(altered, feedConnectionId, opDesc, feedPolicyProperties,
                        FeedRuntimeType.STORE, false, operandId);
                oldNewOID.put(opDesc.getOperatorId(), metaOp.getOperatorId());

            } else {
                FeedRuntimeType runtimeType = null;
                boolean enableSubscriptionMode = false;
                if (opDesc instanceof AlgebricksMetaOperatorDescriptor) {
                    IPushRuntimeFactory runtimeFactory = ((AlgebricksMetaOperatorDescriptor) opDesc).getPipeline()
                            .getRuntimeFactories()[0];
                    if (runtimeFactory instanceof AssignRuntimeFactory) {
                        IConnectorDescriptor connectorDesc = spec.getOperatorInputMap().get(opDesc.getOperatorId())
                                .get(0);
                        IOperatorDescriptor sourceOp = spec.getProducer(connectorDesc);
                        if (sourceOp instanceof FeedCollectOperatorDescriptor) {
                            runtimeType = preProcessingRequired ? FeedRuntimeType.COMPUTE : FeedRuntimeType.OTHER;
                            enableSubscriptionMode = preProcessingRequired;
                        } else {
                            runtimeType = FeedRuntimeType.OTHER;
                        }
                    } else if (runtimeFactory instanceof StreamProjectRuntimeFactory) {
                        runtimeType = FeedRuntimeType.COMMIT;
                    }
                }
                metaOp = new FeedMetaOperatorDescriptor(altered, feedConnectionId, opDesc, feedPolicyProperties,
                        runtimeType, enableSubscriptionMode, operandId);

                oldNewOID.put(opDesc.getOperatorId(), metaOp.getOperatorId());
            }
        }

        // copy connectors
        Map<ConnectorDescriptorId, ConnectorDescriptorId> connectorMapping = new HashMap<ConnectorDescriptorId, ConnectorDescriptorId>();
        for (Entry<ConnectorDescriptorId, IConnectorDescriptor> entry : spec.getConnectorMap().entrySet()) {
            IConnectorDescriptor connDesc = entry.getValue();
            ConnectorDescriptorId newConnId = altered.createConnectorDescriptor(connDesc);
            connectorMapping.put(entry.getKey(), newConnId);
        }

        // make connections between operators
        for (Entry<ConnectorDescriptorId, Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>>> entry : spec
                .getConnectorOperatorMap().entrySet()) {
            IConnectorDescriptor connDesc = altered.getConnectorMap().get(connectorMapping.get(entry.getKey()));
            Pair<IOperatorDescriptor, Integer> leftOp = entry.getValue().getLeft();
            Pair<IOperatorDescriptor, Integer> rightOp = entry.getValue().getRight();

            IOperatorDescriptor leftOpDesc = altered.getOperatorMap().get(
                    oldNewOID.get(leftOp.getLeft().getOperatorId()));
            IOperatorDescriptor rightOpDesc = altered.getOperatorMap().get(
                    oldNewOID.get(rightOp.getLeft().getOperatorId()));

            altered.connect(connDesc, leftOpDesc, leftOp.getRight(), rightOpDesc, rightOp.getRight());
        }

        // prepare for setting partition constraints
        Map<OperatorDescriptorId, List<LocationConstraint>> operatorLocations = new HashMap<OperatorDescriptorId, List<LocationConstraint>>();
        Map<OperatorDescriptorId, Integer> operatorCounts = new HashMap<OperatorDescriptorId, Integer>();

        for (Constraint constraint : spec.getUserConstraints()) {
            LValueConstraintExpression lexpr = constraint.getLValue();
            ConstraintExpression cexpr = constraint.getRValue();
            OperatorDescriptorId opId;
            switch (lexpr.getTag()) {
                case PARTITION_COUNT:
                    opId = ((PartitionCountExpression) lexpr).getOperatorDescriptorId();
                    operatorCounts.put(opId, (int) ((ConstantExpression) cexpr).getValue());
                    break;
                case PARTITION_LOCATION:
                    opId = ((PartitionLocationExpression) lexpr).getOperatorDescriptorId();

                    IOperatorDescriptor opDesc = altered.getOperatorMap().get(oldNewOID.get(opId));
                    List<LocationConstraint> locations = operatorLocations.get(opDesc.getOperatorId());
                    if (locations == null) {
                        locations = new ArrayList<>();
                        operatorLocations.put(opDesc.getOperatorId(), locations);
                    }
                    String location = (String) ((ConstantExpression) cexpr).getValue();
                    LocationConstraint lc = new LocationConstraint();
                    lc.location = location;
                    lc.partition = ((PartitionLocationExpression) lexpr).getPartition();
                    locations.add(lc);
                    break;
            }
        }

        // set absolute location constraints
        for (Entry<OperatorDescriptorId, List<LocationConstraint>> entry : operatorLocations.entrySet()) {
            IOperatorDescriptor opDesc = altered.getOperatorMap().get(oldNewOID.get(entry.getKey()));
            Collections.sort(entry.getValue(), new Comparator<LocationConstraint>() {

                @Override
                public int compare(LocationConstraint o1, LocationConstraint o2) {
                    return o1.partition - o2.partition;
                }
            });
            String[] locations = new String[entry.getValue().size()];
            for (int i = 0; i < locations.length; ++i) {
                locations[i] = entry.getValue().get(i).location;
            }
            PartitionConstraintHelper.addAbsoluteLocationConstraint(altered, opDesc, locations);
        }

        // set count constraints
        for (Entry<OperatorDescriptorId, Integer> entry : operatorCounts.entrySet()) {
            IOperatorDescriptor opDesc = altered.getOperatorMap().get(oldNewOID.get(entry.getKey()));
            if (!operatorLocations.keySet().contains(entry.getKey())) {
                PartitionConstraintHelper.addPartitionCountConstraint(altered, opDesc, entry.getValue());
            }
        }

        // useConnectorSchedulingPolicy
        altered.setUseConnectorPolicyForScheduling(spec.isUseConnectorPolicyForScheduling());

        // connectorAssignmentPolicy
        altered.setConnectorPolicyAssignmentPolicy(spec.getConnectorPolicyAssignmentPolicy());

        // roots
        for (OperatorDescriptorId root : spec.getRoots()) {
            altered.addRoot(altered.getOperatorMap().get(oldNewOID.get(root)));
        }

        // jobEventListenerFactory
        altered.setJobletEventListenerFactory(spec.getJobletEventListenerFactory());

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("New Job Spec:" + altered);
        }

        return altered;

    }

    private static boolean preProcessingRequired(FeedConnectionId connectionId) {
        MetadataTransactionContext ctx = null;
        Feed feed = null;
        boolean preProcessingRequired = false;
        try {
            MetadataManager.INSTANCE.acquireReadLatch();
            ctx = MetadataManager.INSTANCE.beginTransaction();
            feed = MetadataManager.INSTANCE.getFeed(ctx, connectionId.getFeedId().getDataverse(), connectionId
                    .getFeedId().getFeedName());
            preProcessingRequired = feed.getAppliedFunction() != null;
            MetadataManager.INSTANCE.commitTransaction(ctx);
        } catch (Exception e) {
            if (ctx != null) {
                try {
                    MetadataManager.INSTANCE.abortTransaction(ctx);
                } catch (Exception abortException) {
                    e.addSuppressed(abortException);
                    throw new IllegalStateException(e);
                }
            }
        } finally {
            MetadataManager.INSTANCE.releaseReadLatch();
        }
        return preProcessingRequired;
    }

    public static Triple<IAdapterFactory, ARecordType, AdapterType> getPrimaryFeedFactoryAndOutput(PrimaryFeed feed,
            MetadataTransactionContext mdTxnCtx) throws AlgebricksException {

        String adapterName = null;
        DatasourceAdapter adapterEntity = null;
        String adapterFactoryClassname = null;
        IAdapterFactory adapterFactory = null;
        ARecordType adapterOutputType = null;
        Triple<IAdapterFactory, ARecordType, AdapterType> feedProps = null;
        AdapterType adapterType = null;
        try {
            adapterName = feed.getAdaptorName();
            adapterEntity = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, MetadataConstants.METADATA_DATAVERSE_NAME,
                    adapterName);
            if (adapterEntity == null) {
                adapterEntity = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, feed.getDataverseName(), adapterName);
            }
            if (adapterEntity != null) {
                adapterType = adapterEntity.getType();
                adapterFactoryClassname = adapterEntity.getClassname();
                switch (adapterType) {
                    case INTERNAL:
                        adapterFactory = (IAdapterFactory) Class.forName(adapterFactoryClassname).newInstance();
                        break;
                    case EXTERNAL:
                        String[] anameComponents = adapterName.split("#");
                        String libraryName = anameComponents[0];
                        ClassLoader cl = ExternalLibraryManager.getLibraryClassLoader(feed.getDataverseName(),
                                libraryName);
                        adapterFactory = (IAdapterFactory) cl.loadClass(adapterFactoryClassname).newInstance();
                        break;
                }
            } else {
                adapterFactoryClassname = AqlMetadataProvider.adapterFactoryMapping.get(adapterName);
                if (adapterFactoryClassname == null) {
                    adapterFactoryClassname = adapterName;
                }
                adapterFactory = (IAdapterFactory) Class.forName(adapterFactoryClassname).newInstance();
                adapterType = AdapterType.INTERNAL;
            }

            Map<String, String> configuration = feed.getAdaptorConfiguration();

            switch (adapterFactory.getAdapterType()) {
                case TYPED:
                    ((ITypedAdapterFactory) adapterFactory).configure(configuration);
                    adapterOutputType = ((ITypedAdapterFactory) adapterFactory).getAdapterOutputType();
                    break;
                case GENERIC:
                    String outputTypeName = configuration.get(IGenericAdapterFactory.KEY_TYPE_NAME);
                    if (outputTypeName == null) {
                        throw new IllegalArgumentException(
                                "You must specify the datatype associated with the incoming data. Datatype is specified by the "
                                        + IGenericAdapterFactory.KEY_TYPE_NAME + " configuration parameter");
                    }
                    Datatype datatype = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, feed.getDataverseName(),
                            outputTypeName);
                    if (datatype == null) {
                        throw new Exception("no datatype \"" + outputTypeName + "\" in dataverse \""
                                + feed.getDataverseName() + "\"");
                    }
                    adapterOutputType = (ARecordType) datatype.getDatatype();
                    ((IGenericAdapterFactory) adapterFactory).configure(configuration, (ARecordType) adapterOutputType);
                    break;
                default:
                    throw new IllegalStateException(" Unknown factory type for " + adapterFactoryClassname);
            }

            feedProps = new Triple<IAdapterFactory, ARecordType, AdapterType>(adapterFactory, adapterOutputType,
                    adapterType);
        } catch (Exception e) {
            e.printStackTrace();
            throw new AlgebricksException("unable to create adapter  " + e);
        }
        return feedProps;
    }

    public static String getSecondaryFeedOutput(SecondaryFeed feed, MetadataTransactionContext mdTxnCtx)
            throws AlgebricksException, MetadataException {
        String outputType = null;
        String primaryFeedName = feed.getSourceFeedName();
        Feed primaryFeed = MetadataManager.INSTANCE.getFeed(mdTxnCtx, feed.getDataverseName(), primaryFeedName);
        FunctionSignature appliedFunction = primaryFeed.getAppliedFunction();
        if (appliedFunction == null) {
            Triple<IAdapterFactory, ARecordType, AdapterType> result = getPrimaryFeedFactoryAndOutput(
                    (PrimaryFeed) primaryFeed, mdTxnCtx);
            outputType = result.second.getTypeName();
        } else {
            Function function = MetadataManager.INSTANCE.getFunction(mdTxnCtx, appliedFunction);
            if (function != null) {
                if (function.getLanguage().equals(Function.LANGUAGE_AQL)) {
                    throw new NotImplementedException(
                            "Secondary feeds derived from a source feed that has an applied AQL function are not supported yet.");
                } else {
                    outputType = function.getReturnType();
                }
            } else {
                throw new IllegalArgumentException("Function " + appliedFunction
                        + " associated with source feed not found in Metadata.");
            }
        }
        return outputType;
    }

    public static void alterCardinality(JobSpecification spec, FeedRuntimeType compute, int requiredCardinality,
            List<String> helperComputeNodes, List<String> currentLocations) {
        Set<Constraint> userConstraints = spec.getUserConstraints();
        Constraint countConstraint = null;
        Constraint locationConstraint = null;
        List<LocationConstraint> locations = new ArrayList<LocationConstraint>();
        IOperatorDescriptor changingOpDesc = null;

        for (Constraint constraint : userConstraints) {
            LValueConstraintExpression lexpr = constraint.getLValue();
            ConstraintExpression cexpr = constraint.getRValue();
            OperatorDescriptorId opId;
            switch (lexpr.getTag()) {
                case PARTITION_COUNT: {
                    opId = ((PartitionCountExpression) lexpr).getOperatorDescriptorId();
                    IOperatorDescriptor opDesc = spec.getOperatorMap().get(opId);
                    if (opDesc instanceof FeedMetaOperatorDescriptor) {
                        FeedRuntimeType runtimeType = ((FeedMetaOperatorDescriptor) opDesc).getRuntimeType();
                        if (runtimeType.equals(FeedRuntimeType.COMPUTE)) {
                            countConstraint = constraint;
                            changingOpDesc = opDesc;
                        }
                    }
                    break;
                }
                case PARTITION_LOCATION:
                    opId = ((PartitionLocationExpression) lexpr).getOperatorDescriptorId();
                    IOperatorDescriptor opDesc = spec.getOperatorMap().get(opId);
                    if (opDesc instanceof FeedMetaOperatorDescriptor) {
                        FeedRuntimeType runtimeType = ((FeedMetaOperatorDescriptor) opDesc).getRuntimeType();
                        if (runtimeType.equals(FeedRuntimeType.COMPUTE)) {
                            locationConstraint = constraint;
                            changingOpDesc = opDesc;
                            String location = (String) ((ConstantExpression) cexpr).getValue();
                            LocationConstraint lc = new LocationConstraint();
                            lc.location = location;
                            lc.partition = ((PartitionLocationExpression) lexpr).getPartition();
                            locations.add(lc);
                        }
                    }

                    break;
            }
        }

        if (countConstraint != null) {
            userConstraints.remove(countConstraint);
            if (locationConstraint != null) {
                userConstraints.remove(locationConstraint);
            }
            currentLocations.addAll(helperComputeNodes);
            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, changingOpDesc,
                    currentLocations.toArray(new String[] {}));
        }

    }
}
