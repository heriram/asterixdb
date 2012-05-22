/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.file;

import java.rmi.RemoteException;
import java.util.List;
import java.util.logging.Logger;

import edu.uci.ics.asterix.api.common.Job;
import edu.uci.ics.asterix.aql.translator.DdlTranslator.CompiledDatasetDropStatement;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.config.OptimizationConfUtil;
import edu.uci.ics.asterix.common.context.AsterixIndexRegistryProvider;
import edu.uci.ics.asterix.common.context.AsterixStorageManagerInterface;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.base.IDataFormat;

import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryHashFunctionFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlTypeTraitProvider;
import edu.uci.ics.asterix.kvs.KVRequestDispatcherOperatorDescriptor;
import edu.uci.ics.asterix.kvs.KVRequestHandlerOperatorDescriptor;
import edu.uci.ics.asterix.kvs.KVSResponseDispatcherOperatorDescriptor;
import edu.uci.ics.asterix.kvs.KVUtils;
import edu.uci.ics.asterix.kvs.MToNPartitioningTimeTriggeredConnectorDescriptor;
import edu.uci.ics.asterix.metadata.MetadataException;

import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledExternalDatasetDetails;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledInternalDatasetDetails;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.translator.DmlTranslator.CompiledLoadFromFileStatement;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;

import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;

//import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IPushRuntimeFactory;
//import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.jobgen.impl.JobGenHelper;
//import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
//import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.operators.std.AssignRuntimeFactory;
//import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.ConnectorPolicyAssignmentPolicy;

import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.ConnectorPolicyAssignmentPolicy;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import edu.uci.ics.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningMergingConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexCreateOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexDropOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public class DatasetOperations {

    private static final PhysicalOptimizationConfig physicalOptimizationConfig = OptimizationConfUtil
            .getPhysicalOptimizationConfig();

    private static Logger LOGGER = Logger.getLogger(DatasetOperations.class.getName());

    public static JobSpecification[] createDropDatasetJobSpec(CompiledDatasetDropStatement deleteStmt,
            AqlCompiledMetadataDeclarations metadata) throws AlgebricksException, HyracksDataException,
            RemoteException, ACIDException, AsterixException {

        String datasetName = deleteStmt.getDatasetName();
        String datasetPath = metadata.getRelativePath(datasetName);

        LOGGER.info("DROP DATASETPATH: " + datasetPath);

        IIndexRegistryProvider<IIndex> indexRegistryProvider = AsterixIndexRegistryProvider.INSTANCE;
        IStorageManagerInterface storageManager = AsterixStorageManagerInterface.INSTANCE;

        AqlCompiledDatasetDecl adecl = metadata.findDataset(datasetName);
        if (adecl == null) {
            throw new AlgebricksException("DROP DATASET: No metadata for dataset " + datasetName);
        }
        if (adecl.getDatasetType() == DatasetType.EXTERNAL) {
            return new JobSpecification[0];
        }

        List<AqlCompiledIndexDecl> secondaryIndexes = DatasetUtils.getSecondaryIndexes(adecl);

        JobSpecification[] specs;

        if (secondaryIndexes != null && !secondaryIndexes.isEmpty()) {
            int n = secondaryIndexes.size();
            specs = new JobSpecification[n + 1];
            int i = 0;
            // First, drop secondary indexes.
            for (AqlCompiledIndexDecl acid : secondaryIndexes) {
                specs[i] = new JobSpecification();
                Pair<IFileSplitProvider, AlgebricksPartitionConstraint> idxSplitsAndConstraint = metadata
                        .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, acid.getIndexName());
                TreeIndexDropOperatorDescriptor secondaryBtreeDrop = new TreeIndexDropOperatorDescriptor(specs[i],
                        storageManager, indexRegistryProvider, idxSplitsAndConstraint.first);
                AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(specs[i], secondaryBtreeDrop,
                        idxSplitsAndConstraint.second);
                i++;
            }
        } else {
            specs = new JobSpecification[1];
        }
        JobSpecification specPrimary = new JobSpecification();
        specs[specs.length - 1] = specPrimary;

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = metadata
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, datasetName);
        TreeIndexDropOperatorDescriptor primaryBtreeDrop = new TreeIndexDropOperatorDescriptor(specPrimary,
                storageManager, indexRegistryProvider, splitsAndConstraint.first);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(specPrimary, primaryBtreeDrop,
                splitsAndConstraint.second);

        specPrimary.addRoot(primaryBtreeDrop);

        return specs;
    }

    // TODO: Lots of common code in this file. Refactor everything after merging in asterix-fix-issue-9.
    public static JobSpecification createDatasetJobSpec(String datasetName,
            AqlCompiledMetadataDeclarations metadata) throws AsterixException, AlgebricksException {AqlCompiledDatasetDecl compiledDatasetDecl = metadata.findDataset(datasetName);
            if (compiledDatasetDecl == null) {
                throw new AsterixException("Could not find dataset " + datasetName);
            }
            JobSpecification spec = new JobSpecification();
            IBinaryComparatorFactory[] comparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(
                    compiledDatasetDecl, metadata.getFormat().getBinaryComparatorFactoryProvider());
            ITypeTraits[] typeTraits = DatasetUtils.computeTupleTypeTraits(compiledDatasetDecl, metadata);
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = metadata
                    .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, datasetName);
            FileSplit[] fs = splitsAndConstraint.first.getFileSplits();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < fs.length; i++) {
                sb.append(stringOf(fs[i]) + " ");
            }
            LOGGER.info("CREATING File Splits: " + sb.toString());
            IIndexRegistryProvider<IIndex> indexRegistryProvider = AsterixIndexRegistryProvider.INSTANCE;
            IStorageManagerInterface storageManager = AsterixStorageManagerInterface.INSTANCE;
            TreeIndexCreateOperatorDescriptor indexCreateOp = new TreeIndexCreateOperatorDescriptor(spec,
                    storageManager, indexRegistryProvider, splitsAndConstraint.first, typeTraits, comparatorFactories,
                    new BTreeDataflowHelperFactory(), NoOpOperationCallbackProvider.INSTANCE);
            AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, indexCreateOp,
                    splitsAndConstraint.second);
            spec.addRoot(indexCreateOp);
            return spec;
    }
    
    public static JobSpecification createKeyValueServiceJobSpec(String datasetName, AqlCompiledMetadataDeclarations metadata) throws Exception{
    	 AqlCompiledDatasetDecl compiledDatasetDecl = metadata.findDataset(datasetName);
    	 if (compiledDatasetDecl == null) {
             throw new AsterixException("Could not find dataset " + datasetName + " for keyservice registration");
         }
         if (compiledDatasetDecl.getDatasetType() != DatasetType.INTERNAL
                 && compiledDatasetDecl.getDatasetType() != DatasetType.FEED) {
             throw new AsterixException("Cannot register  dataset  (" + datasetName + ")" + "of type "
                     + compiledDatasetDecl.getDatasetType() + " for keyservice");
         }
         
        AqlCompiledInternalDatasetDetails acidd = (AqlCompiledInternalDatasetDetails) compiledDatasetDecl.getAqlCompiledDatasetDetails();
 		List<Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType>> pfList = acidd.getPartitioningFunctions();
 		
 		int i=0;
 		IAType[] keyTypes = new IAType[pfList.size()];
 		for(Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> t : pfList){
 			keyTypes[i] = t.third;
 			i++;
 		}
 		
 		List<String> partitionKeys = acidd.getPartitioningExprs();
 		String ixName = DatasetUtils.getPrimaryIndex(compiledDatasetDecl).getIndexName();
 		Pair<IFileSplitProvider, AlgebricksPartitionConstraint> fsap = metadata.splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, ixName);
 		ConstantFileSplitProvider fs = (ConstantFileSplitProvider) fsap.first;
		AlgebricksAbsolutePartitionConstraint partConst = (AlgebricksAbsolutePartitionConstraint) fsap.second;
		
		String itemTypeName = compiledDatasetDecl.getItemTypeName();
		IAType itemType = metadata.findType(itemTypeName);
		ARecordType record = ((ARecordType) itemType);
		
		//----
		int numPrimaryKeys = DatasetUtils.getPartitioningFunctions(compiledDatasetDecl).size();
		
		ITypeTraits[] typeTraits = new ITypeTraits[numPrimaryKeys + 1];
        ISerializerDeserializer[] recordFields = new ISerializerDeserializer[numPrimaryKeys + 1];
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[numPrimaryKeys];
        
        ISerializerDeserializer payloadSerde = metadata.getFormat().getSerdeProvider().getSerializerDeserializer(itemType);	
        recordFields[numPrimaryKeys] = payloadSerde;	
        typeTraits[numPrimaryKeys] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(itemType);
        
        int j = 0;
        for (Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> evalFactoryAndType : DatasetUtils.getPartitioningFunctions(compiledDatasetDecl)) {
            IAType keyType = evalFactoryAndType.third;
            ISerializerDeserializer keySerde = metadata.getFormat().getSerdeProvider()
                    .getSerializerDeserializer(keyType);
            recordFields[j] = keySerde;    
            comparatorFactories[j] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(keyType, true);
            typeTraits[j] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
            ++j;
        }
        
        Triple<ITypeTraits[], IBinaryComparatorFactory[], ISerializerDeserializer[]> triple = new Triple<ITypeTraits[], IBinaryComparatorFactory[], ISerializerDeserializer[]>(typeTraits, comparatorFactories, recordFields);
		
		//----
        
        ITypeTraits[] tt = triple.first;
		IBinaryComparatorFactory[] bcf = triple.second;
		ISerializerDeserializer[] isd = triple.third;
		
		String dataverseName = metadata.getDataverseName();
		
		JobSpecification spec = generateKeyServiceJobSpec(dataverseName, datasetName, keyTypes, tt, bcf, fs, isd, record, partitionKeys, partConst);
		return spec;
    }
    
    @SuppressWarnings("unchecked")
    public static Job createLoadDatasetJobSpec(CompiledLoadFromFileStatement loadStmt,
            AqlCompiledMetadataDeclarations metadata) throws AsterixException, AlgebricksException {
        String datasetName = loadStmt.getDatasetName();
        AqlCompiledDatasetDecl compiledDatasetDecl = metadata.findDataset(datasetName);
        if (compiledDatasetDecl == null) {
            throw new AsterixException("Could not find dataset " + datasetName);
        }
        if (compiledDatasetDecl.getDatasetType() != DatasetType.INTERNAL
                && compiledDatasetDecl.getDatasetType() != DatasetType.FEED) {
            throw new AsterixException("Cannot load data into dataset  (" + datasetName + ")" + "of type "
                    + compiledDatasetDecl.getDatasetType());
        }
        JobSpecification spec = new JobSpecification();

        ARecordType itemType = (ARecordType) metadata.findType(compiledDatasetDecl.getItemTypeName());
        IDataFormat format = metadata.getFormat();
        ISerializerDeserializer payloadSerde = format.getSerdeProvider().getSerializerDeserializer(itemType);

        IBinaryHashFunctionFactory[] hashFactories = DatasetUtils.computeKeysBinaryHashFunFactories(
                compiledDatasetDecl, metadata.getFormat().getBinaryHashFunctionFactoryProvider());
        IBinaryComparatorFactory[] comparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(
                compiledDatasetDecl, metadata.getFormat().getBinaryComparatorFactoryProvider());
        ITypeTraits[] typeTraits = DatasetUtils.computeTupleTypeTraits(compiledDatasetDecl, metadata);

        AqlCompiledExternalDatasetDetails externalDatasetDetails = new AqlCompiledExternalDatasetDetails(
                loadStmt.getAdapter(), loadStmt.getProperties());
        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> p = AqlMetadataProvider
                .buildExternalDataScannerRuntime(spec, itemType, externalDatasetDetails, format);
        IOperatorDescriptor scanner = p.first;
        AlgebricksPartitionConstraint scannerPc = p.second;
        RecordDescriptor recDesc = computePayloadKeyRecordDescriptor(compiledDatasetDecl, payloadSerde,
                metadata.getFormat());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, scanner, scannerPc);

        AssignRuntimeFactory assign = makeAssignRuntimeFactory(compiledDatasetDecl);
        AlgebricksMetaOperatorDescriptor asterixOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1,
                new IPushRuntimeFactory[] { assign }, new RecordDescriptor[] { recDesc });

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, asterixOp, scannerPc);

        List<Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType>> partitioningFunctions = DatasetUtils
                .getPartitioningFunctions(compiledDatasetDecl);
        int numKeys = partitioningFunctions.size();
        int[] keys = new int[numKeys];
        for (int i = 0; i < numKeys; i++) {
            keys[i] = i + 1;
        }
        // Move key fields to front.
        int[] fieldPermutation = new int[numKeys + 1];
        for (int i = 0; i < numKeys; i++) {
            fieldPermutation[i] = i + 1;
        }
        fieldPermutation[numKeys] = 0;

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = metadata
                .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, datasetName);

        FileSplit[] fs = splitsAndConstraint.first.getFileSplits();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fs.length; i++) {
            sb.append(stringOf(fs[i]) + " ");
        }
        LOGGER.info("LOAD into File Splits: " + sb.toString());

        IIndexRegistryProvider<IIndex> indexRegistryProvider = AsterixIndexRegistryProvider.INSTANCE;
        IStorageManagerInterface storageManager = AsterixStorageManagerInterface.INSTANCE;
        TreeIndexBulkLoadOperatorDescriptor btreeBulkLoad = new TreeIndexBulkLoadOperatorDescriptor(spec,
                storageManager, indexRegistryProvider, splitsAndConstraint.first, typeTraits, comparatorFactories,
                fieldPermutation, GlobalConfig.DEFAULT_BTREE_FILL_FACTOR, new BTreeDataflowHelperFactory(),
                NoOpOperationCallbackProvider.INSTANCE);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, btreeBulkLoad,
                splitsAndConstraint.second);

        spec.connect(new OneToOneConnectorDescriptor(spec), scanner, 0, asterixOp, 0);

        if (!loadStmt.alreadySorted()) {
            int framesLimit = physicalOptimizationConfig.getMaxFramesExternalSort();
            ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, framesLimit, keys,
                    comparatorFactories, recDesc);
            AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, sorter,
                    splitsAndConstraint.second);
            IConnectorDescriptor hashConn = new MToNPartitioningConnectorDescriptor(spec,
                    new FieldHashPartitionComputerFactory(keys, hashFactories));
            spec.connect(hashConn, asterixOp, 0, sorter, 0);
            spec.connect(new OneToOneConnectorDescriptor(spec), sorter, 0, btreeBulkLoad, 0);
        } else {
            IConnectorDescriptor sortMergeConn = new MToNPartitioningMergingConnectorDescriptor(spec,
                    new FieldHashPartitionComputerFactory(keys, hashFactories), keys, comparatorFactories);
            spec.connect(sortMergeConn, asterixOp, 0, btreeBulkLoad, 0);
        }
        spec.addRoot(btreeBulkLoad);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());

        return new Job(spec);
    }

    private static String stringOf(FileSplit fs) {
        return fs.getNodeName() + ":" + fs.getLocalFile().toString();
    }

    private static AssignRuntimeFactory makeAssignRuntimeFactory(AqlCompiledDatasetDecl compiledDatasetDecl) {
        List<Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType>> partitioningFunctions = DatasetUtils
                .getPartitioningFunctions(compiledDatasetDecl);
        int numKeys = partitioningFunctions.size();
        IEvaluatorFactory[] evalFactories = new IEvaluatorFactory[numKeys];
        for (int i = 0; i < numKeys; i++) {
            Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> evalFactoryAndType = partitioningFunctions
                    .get(i);
            evalFactories[i] = evalFactoryAndType.first;
        }
        int[] outColumns = new int[numKeys];
        int[] projectionList = new int[numKeys + 1];
        projectionList[0] = 0;

        for (int i = 0; i < numKeys; i++) {
            outColumns[i] = i + 1;
            projectionList[i + 1] = i + 1;
        }
        return new AssignRuntimeFactory(outColumns, evalFactories, projectionList);
    }

    private static RecordDescriptor computePayloadKeyRecordDescriptor(AqlCompiledDatasetDecl compiledDatasetDecl,
            ISerializerDeserializer payloadSerde, IDataFormat dataFormat) throws AlgebricksException {
        List<Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType>> partitioningFunctions = DatasetUtils
                .getPartitioningFunctions(compiledDatasetDecl);
        int numKeys = partitioningFunctions.size();
        ISerializerDeserializer[] recordFields = new ISerializerDeserializer[1 + numKeys];
        recordFields[0] = payloadSerde;
        for (int i = 0; i < numKeys; i++) {
            Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> evalFactoryAndType = partitioningFunctions
                    .get(i);
            IAType keyType = evalFactoryAndType.third;
            ISerializerDeserializer keySerde = dataFormat.getSerdeProvider().getSerializerDeserializer(keyType);
            recordFields[i + 1] = keySerde;
        }
        return new RecordDescriptor(recordFields);
    }
    
    private static JobSpecification generateKeyServiceJobSpec(String dvName, String dsName, IAType[] keyType, ITypeTraits[] typeTraits, IBinaryComparatorFactory[] comparatorFactories, IFileSplitProvider fileSplitProvider, ISerializerDeserializer[] res, ARecordType record, List<String> partitioningKeys, AlgebricksAbsolutePartitionConstraint parts) throws Exception{
		
		JobSpecification spec = new JobSpecification();
		KVRequestDispatcherOperatorDescriptor reqDisp = 
			new KVRequestDispatcherOperatorDescriptor(spec, keyType, dvName, dsName, record, partitioningKeys, 1);
		
		IStorageManagerInterface storageManager = AsterixStorageManagerInterface.INSTANCE;
		IIndexRegistryProvider<IIndex> indexRegistryProvider = AsterixIndexRegistryProvider.INSTANCE;
		
		//ITreeIndexFrameFactory interiorFrameFactory = KVUtils.getInteriorFrameFactory(typeTraits); 
	    //ITreeIndexFrameFactory leafFrameFactory = KVUtils.getLeafFrameFactory(typeTraits); 
	    
	    ISerializerDeserializer[] kvRespSerDe = new ISerializerDeserializer[res.length + 2];
	    kvRespSerDe[0] = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
	    kvRespSerDe[1] = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
	    for(int i=2; i<kvRespSerDe.length; i++){
	    	kvRespSerDe[i] = res[i-2];
	    }
	    RecordDescriptor kvRespRecDesc = new RecordDescriptor(kvRespSerDe);
	    
		
		KVRequestHandlerOperatorDescriptor reqHandler = 
				new KVRequestHandlerOperatorDescriptor(spec, kvRespRecDesc, 
						storageManager, indexRegistryProvider, fileSplitProvider, 
							/*interiorFrameFactory, leafFrameFactory,*/ typeTraits, 
								comparatorFactories, new BTreeDataflowHelperFactory(), keyType.length, 1);
		
		
		KVSResponseDispatcherOperatorDescriptor respDisp = new KVSResponseDispatcherOperatorDescriptor(spec);
		
		
		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, reqDisp, parts.getLocations() );
		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, reqHandler, parts.getLocations() );
		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, respDisp, parts.getLocations() );
		
		
		IBinaryHashFunctionFactory[] hashFactories1 = new IBinaryHashFunctionFactory[keyType.length];
		int[] keysIx = new int[keyType.length];
		for(int i=0; i<keysIx.length; i++){
			keysIx[i] = (3+i);
			hashFactories1[i] = AqlBinaryHashFunctionFactoryProvider.INSTANCE.getBinaryHashFunctionFactory(keyType[i] );	
		}
		ITuplePartitionComputerFactory tpcf1 = new FieldHashPartitionComputerFactory(keysIx, hashFactories1);
		IConnectorDescriptor con1 = new MToNPartitioningTimeTriggeredConnectorDescriptor(spec, tpcf1, 1);
		
		IBinaryHashFunctionFactory[] hashFactories2 = new IBinaryHashFunctionFactory[]{AqlBinaryHashFunctionFactoryProvider.INSTANCE.getBinaryHashFunctionFactory(BuiltinType.AINT32)};	
		ITuplePartitionComputerFactory tpcf2 = new FieldHashPartitionComputerFactory(new int[]{0}, hashFactories2);
		IConnectorDescriptor con2 = new MToNPartitioningTimeTriggeredConnectorDescriptor(spec, tpcf2, 1);
		
		spec.connect(con1, reqDisp, 0, reqHandler, 0);
	    spec.connect(con2, reqHandler, 0, respDisp, 0);
	    spec.addRoot(respDisp); 
	    return spec;
	}
    

}
