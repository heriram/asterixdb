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
package edu.uci.ics.asterix.runtime.formats;

import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.exceptions.AsterixRuntimeException;
import edu.uci.ics.asterix.common.parse.IParseFileSplitsDecl;
import edu.uci.ics.asterix.dataflow.data.nontagged.AqlNullWriterFactory;
import edu.uci.ics.asterix.formats.base.IDataFormat;
import edu.uci.ics.asterix.formats.nontagged.*;
import edu.uci.ics.asterix.om.base.*;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.*;
import edu.uci.ics.asterix.om.pointables.base.DefaultOpenFieldType;
import edu.uci.ics.asterix.om.typecomputer.base.TypeComputerUtilities;
import edu.uci.ics.asterix.om.types.*;
import edu.uci.ics.asterix.runtime.aggregates.collections.ListifyAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.scalar.*;
import edu.uci.ics.asterix.runtime.aggregates.serializable.std.*;
import edu.uci.ics.asterix.runtime.aggregates.std.*;
import edu.uci.ics.asterix.runtime.aggregates.stream.EmptyStreamAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.stream.NonEmptyStreamAggregateDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.accessors.*;
import edu.uci.ics.asterix.runtime.evaluators.common.CreateMBREvalFactory;
import edu.uci.ics.asterix.runtime.evaluators.common.FieldAccessByIndexEvalFactory;
import edu.uci.ics.asterix.runtime.evaluators.common.FieldAccessNestedEvalFactory;
import edu.uci.ics.asterix.runtime.evaluators.common.FunctionManagerImpl;
import edu.uci.ics.asterix.runtime.evaluators.constructors.*;
import edu.uci.ics.asterix.runtime.evaluators.functions.*;
import edu.uci.ics.asterix.runtime.evaluators.functions.binary.*;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.*;
import edu.uci.ics.asterix.runtime.operators.file.AdmSchemafullRecordParserFactory;
import edu.uci.ics.asterix.runtime.operators.file.NtDelimitedDataTupleParserFactory;
import edu.uci.ics.asterix.runtime.runningaggregates.std.TidRunningAggregateDescriptor;
import edu.uci.ics.asterix.runtime.unnestingfunctions.std.RangeDescriptor;
import edu.uci.ics.asterix.runtime.unnestingfunctions.std.ScanCollectionDescriptor;
import edu.uci.ics.asterix.runtime.unnestingfunctions.std.SubsetCollectionDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.*;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.data.*;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import edu.uci.ics.hyracks.algebricks.runtime.evaluators.ConstantEvalFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IPredicateEvaluatorFactoryProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.*;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class NonTaggedDataFormat implements IDataFormat {

    private static boolean registered = false;

    public static final NonTaggedDataFormat INSTANCE = new NonTaggedDataFormat();

    private static LogicalVariable METADATA_DUMMY_VAR = new LogicalVariable(-1);

    private static final HashMap<ATypeTag, IValueParserFactory> typeToValueParserFactMap = new HashMap<ATypeTag, IValueParserFactory>();

    public static final String NON_TAGGED_DATA_FORMAT = "edu.uci.ics.asterix.runtime.formats.NonTaggedDataFormat";

    static {
        typeToValueParserFactMap.put(ATypeTag.INT32, IntegerParserFactory.INSTANCE);
        typeToValueParserFactMap.put(ATypeTag.FLOAT, FloatParserFactory.INSTANCE);
        typeToValueParserFactMap.put(ATypeTag.DOUBLE, DoubleParserFactory.INSTANCE);
        typeToValueParserFactMap.put(ATypeTag.INT64, LongParserFactory.INSTANCE);
        typeToValueParserFactMap.put(ATypeTag.STRING, UTF8StringParserFactory.INSTANCE);
    }

    public NonTaggedDataFormat() {
    }

    @Override
    public void registerRuntimeFunctions() throws AlgebricksException {

        if (registered) {
            return;
        }
        registered = true;

        if (FunctionManagerHolder.getFunctionManager() != null) {
            return;
        }

        List<IFunctionDescriptorFactory> temp = new ArrayList<IFunctionDescriptorFactory>();

        // format-independent
        temp.add(ContainsDescriptor.FACTORY);
        temp.add(EndsWithDescriptor.FACTORY);
        temp.add(StartsWithDescriptor.FACTORY);
        temp.add(SubstringDescriptor.FACTORY);
        temp.add(TidRunningAggregateDescriptor.FACTORY);

        // format-dependent
        temp.add(AndDescriptor.FACTORY);
        temp.add(OrDescriptor.FACTORY);
        temp.add(LikeDescriptor.FACTORY);
        temp.add(ScanCollectionDescriptor.FACTORY);
        temp.add(AnyCollectionMemberDescriptor.FACTORY);
        temp.add(ClosedRecordConstructorDescriptor.FACTORY);
        temp.add(FieldAccessByIndexDescriptor.FACTORY);
        temp.add(FieldAccessByNameDescriptor.FACTORY);
        temp.add(FieldAccessNestedDescriptor.FACTORY);
        temp.add(GetItemDescriptor.FACTORY);
        temp.add(NumericUnaryMinusDescriptor.FACTORY);
        temp.add(OpenRecordConstructorDescriptor.FACTORY);
        temp.add(OrderedListConstructorDescriptor.FACTORY);
        temp.add(UnorderedListConstructorDescriptor.FACTORY);
        temp.add(EmbedTypeDescriptor.FACTORY);

        temp.add(NumericAddDescriptor.FACTORY);
        temp.add(NumericDivideDescriptor.FACTORY);
        temp.add(NumericMultiplyDescriptor.FACTORY);
        temp.add(NumericSubDescriptor.FACTORY);
        temp.add(NumericModuloDescriptor.FACTORY);
        temp.add(NumericCaretDescriptor.FACTORY);
        temp.add(IsNullDescriptor.FACTORY);
        temp.add(IsSystemNullDescriptor.FACTORY);
        temp.add(NotDescriptor.FACTORY);
        temp.add(LenDescriptor.FACTORY);
        temp.add(EmptyStreamAggregateDescriptor.FACTORY);
        temp.add(NonEmptyStreamAggregateDescriptor.FACTORY);
        temp.add(RangeDescriptor.FACTORY);

        temp.add(NumericAbsDescriptor.FACTORY);
        temp.add(NumericCeilingDescriptor.FACTORY);
        temp.add(NumericFloorDescriptor.FACTORY);
        temp.add(NumericRoundDescriptor.FACTORY);
        temp.add(NumericRoundHalfToEvenDescriptor.FACTORY);
        temp.add(NumericRoundHalfToEven2Descriptor.FACTORY);

        // Binary functions
        temp.add(BinaryLengthDescriptor.FACTORY);
        temp.add(ParseBinaryDescriptor.FACTORY);
        temp.add(PrintBinaryDescriptor.FACTORY);
        temp.add(BinaryConcatDescriptor.FACTORY);
        temp.add(SubBinaryFromDescriptor.FACTORY);
        temp.add(SubBinaryFromToDescriptor.FACTORY);
        temp.add(FindBinaryDescriptor.FACTORY);
        temp.add(FindBinaryFromDescriptor.FACTORY);

        // String functions
        temp.add(StringEqualDescriptor.FACTORY);
        temp.add(StringStartWithDescrtiptor.FACTORY);
        temp.add(StringEndWithDescrtiptor.FACTORY);
        temp.add(StringMatchesDescriptor.FACTORY);
        temp.add(StringLowerCaseDescriptor.FACTORY);
        temp.add(StringUpperCaseDescriptor.FACTORY);
        temp.add(StringMatchesWithFlagDescriptor.FACTORY);
        temp.add(StringReplaceDescriptor.FACTORY);
        temp.add(StringReplaceWithFlagsDescriptor.FACTORY);
        temp.add(StringLengthDescriptor.FACTORY);
        temp.add(Substring2Descriptor.FACTORY);
        temp.add(SubstringBeforeDescriptor.FACTORY);
        temp.add(SubstringAfterDescriptor.FACTORY);
        temp.add(StringToCodePointDescriptor.FACTORY);
        temp.add(CodePointToStringDescriptor.FACTORY);
        temp.add(StringConcatDescriptor.FACTORY);
        temp.add(StringJoinDescriptor.FACTORY);

        // aggregates
        temp.add(ListifyAggregateDescriptor.FACTORY);
        temp.add(CountAggregateDescriptor.FACTORY);
        temp.add(AvgAggregateDescriptor.FACTORY);
        temp.add(LocalAvgAggregateDescriptor.FACTORY);
        temp.add(IntermediateAvgAggregateDescriptor.FACTORY);
        temp.add(GlobalAvgAggregateDescriptor.FACTORY);
        temp.add(SumAggregateDescriptor.FACTORY);
        temp.add(LocalSumAggregateDescriptor.FACTORY);
        temp.add(MaxAggregateDescriptor.FACTORY);
        temp.add(LocalMaxAggregateDescriptor.FACTORY);
        temp.add(MinAggregateDescriptor.FACTORY);
        temp.add(LocalMinAggregateDescriptor.FACTORY);

        // serializable aggregates
        temp.add(SerializableCountAggregateDescriptor.FACTORY);
        temp.add(SerializableAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableLocalAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableIntermediateAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableGlobalAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableSumAggregateDescriptor.FACTORY);
        temp.add(SerializableLocalSumAggregateDescriptor.FACTORY);

        // scalar aggregates
        temp.add(ScalarCountAggregateDescriptor.FACTORY);
        temp.add(ScalarAvgAggregateDescriptor.FACTORY);
        temp.add(ScalarSumAggregateDescriptor.FACTORY);
        temp.add(ScalarMaxAggregateDescriptor.FACTORY);
        temp.add(ScalarMinAggregateDescriptor.FACTORY);

        // SQL aggregates
        temp.add(SqlCountAggregateDescriptor.FACTORY);
        temp.add(SqlAvgAggregateDescriptor.FACTORY);
        temp.add(LocalSqlAvgAggregateDescriptor.FACTORY);
        temp.add(IntermediateSqlAvgAggregateDescriptor.FACTORY);
        temp.add(GlobalSqlAvgAggregateDescriptor.FACTORY);
        temp.add(SqlSumAggregateDescriptor.FACTORY);
        temp.add(LocalSqlSumAggregateDescriptor.FACTORY);
        temp.add(SqlMaxAggregateDescriptor.FACTORY);
        temp.add(LocalSqlMaxAggregateDescriptor.FACTORY);
        temp.add(SqlMinAggregateDescriptor.FACTORY);
        temp.add(LocalSqlMinAggregateDescriptor.FACTORY);

        // SQL serializable aggregates
        temp.add(SerializableSqlCountAggregateDescriptor.FACTORY);
        temp.add(SerializableSqlAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableLocalSqlAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableIntermediateSqlAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableGlobalSqlAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableSqlSumAggregateDescriptor.FACTORY);
        temp.add(SerializableLocalSqlSumAggregateDescriptor.FACTORY);

        // SQL scalar aggregates
        temp.add(ScalarSqlCountAggregateDescriptor.FACTORY);
        temp.add(ScalarSqlAvgAggregateDescriptor.FACTORY);
        temp.add(ScalarSqlSumAggregateDescriptor.FACTORY);
        temp.add(ScalarSqlMaxAggregateDescriptor.FACTORY);
        temp.add(ScalarSqlMinAggregateDescriptor.FACTORY);

        // new functions - constructors
        temp.add(ABooleanConstructorDescriptor.FACTORY);
        temp.add(ANullConstructorDescriptor.FACTORY);
        temp.add(ABinaryHexStringConstructorDescriptor.FACTORY);
        temp.add(ABinaryBase64StringConstructorDescriptor.FACTORY);
        temp.add(AStringConstructorDescriptor.FACTORY);
        temp.add(AInt8ConstructorDescriptor.FACTORY);
        temp.add(AInt16ConstructorDescriptor.FACTORY);
        temp.add(AInt32ConstructorDescriptor.FACTORY);
        temp.add(AInt64ConstructorDescriptor.FACTORY);
        temp.add(AFloatConstructorDescriptor.FACTORY);
        temp.add(ADoubleConstructorDescriptor.FACTORY);
        temp.add(APointConstructorDescriptor.FACTORY);
        temp.add(APoint3DConstructorDescriptor.FACTORY);
        temp.add(ALineConstructorDescriptor.FACTORY);
        temp.add(APolygonConstructorDescriptor.FACTORY);
        temp.add(ACircleConstructorDescriptor.FACTORY);
        temp.add(ARectangleConstructorDescriptor.FACTORY);
        temp.add(ATimeConstructorDescriptor.FACTORY);
        temp.add(ADateConstructorDescriptor.FACTORY);
        temp.add(ADateTimeConstructorDescriptor.FACTORY);
        temp.add(ADurationConstructorDescriptor.FACTORY);
        temp.add(AYearMonthDurationConstructorDescriptor.FACTORY);
        temp.add(ADayTimeDurationConstructorDescriptor.FACTORY);

        temp.add(CreateUUIDDescriptor.FACTORY);
        // Spatial
        temp.add(CreatePointDescriptor.FACTORY);
        temp.add(CreateLineDescriptor.FACTORY);
        temp.add(CreatePolygonDescriptor.FACTORY);
        temp.add(CreateCircleDescriptor.FACTORY);
        temp.add(CreateRectangleDescriptor.FACTORY);
        temp.add(SpatialAreaDescriptor.FACTORY);
        temp.add(SpatialDistanceDescriptor.FACTORY);
        temp.add(SpatialIntersectDescriptor.FACTORY);
        temp.add(CreateMBRDescriptor.FACTORY);
        temp.add(SpatialCellDescriptor.FACTORY);
        temp.add(PointXCoordinateAccessor.FACTORY);
        temp.add(PointYCoordinateAccessor.FACTORY);
        temp.add(CircleRadiusAccessor.FACTORY);
        temp.add(CircleCenterAccessor.FACTORY);
        temp.add(LineRectanglePolygonAccessor.FACTORY);

        // fuzzyjoin function
        temp.add(FuzzyEqDescriptor.FACTORY);
        temp.add(SubsetCollectionDescriptor.FACTORY);
        temp.add(PrefixLenJaccardDescriptor.FACTORY);

        temp.add(WordTokensDescriptor.FACTORY);
        temp.add(HashedWordTokensDescriptor.FACTORY);
        temp.add(CountHashedWordTokensDescriptor.FACTORY);

        temp.add(GramTokensDescriptor.FACTORY);
        temp.add(HashedGramTokensDescriptor.FACTORY);
        temp.add(CountHashedGramTokensDescriptor.FACTORY);

        temp.add(EditDistanceDescriptor.FACTORY);
        temp.add(EditDistanceCheckDescriptor.FACTORY);
        temp.add(EditDistanceStringIsFilterable.FACTORY);
        temp.add(EditDistanceListIsFilterable.FACTORY);
        temp.add(EditDistanceContainsDescriptor.FACTORY);

        temp.add(SimilarityJaccardDescriptor.FACTORY);
        temp.add(SimilarityJaccardCheckDescriptor.FACTORY);
        temp.add(SimilarityJaccardSortedDescriptor.FACTORY);
        temp.add(SimilarityJaccardSortedCheckDescriptor.FACTORY);
        temp.add(SimilarityJaccardPrefixDescriptor.FACTORY);
        temp.add(SimilarityJaccardPrefixCheckDescriptor.FACTORY);

        //Record functions
        temp.add(RecordMergeDescriptor.FACTORY);
        temp.add(RecordAddFieldsDescriptor.FACTORY);
        temp.add(RecordRemoveFieldsDescriptor.FACTORY);
        temp.add(SwitchCaseDescriptor.FACTORY);
        temp.add(RegExpDescriptor.FACTORY);
        temp.add(InjectFailureDescriptor.FACTORY);
        temp.add(CastListDescriptor.FACTORY);
        temp.add(CastRecordDescriptor.FACTORY);
        temp.add(FlowRecordDescriptor.FACTORY);
        temp.add(NotNullDescriptor.FACTORY);

        // Spatial and temporal type accessors
        temp.add(TemporalYearAccessor.FACTORY);
        temp.add(TemporalMonthAccessor.FACTORY);
        temp.add(TemporalDayAccessor.FACTORY);
        temp.add(TemporalHourAccessor.FACTORY);
        temp.add(TemporalMinuteAccessor.FACTORY);
        temp.add(TemporalSecondAccessor.FACTORY);
        temp.add(TemporalMillisecondAccessor.FACTORY);
        temp.add(TemporalIntervalStartAccessor.FACTORY);
        temp.add(TemporalIntervalEndAccessor.FACTORY);
        temp.add(TemporalIntervalStartDateAccessor.FACTORY);
        temp.add(TemporalIntervalEndDateAccessor.FACTORY);
        temp.add(TemporalIntervalStartTimeAccessor.FACTORY);
        temp.add(TemporalIntervalEndTimeAccessor.FACTORY);
        temp.add(TemporalIntervalStartDatetimeAccessor.FACTORY);
        temp.add(TemporalIntervalEndDatetimeAccessor.FACTORY);

        // Temporal functions
        temp.add(DateFromUnixTimeInDaysDescriptor.FACTORY);
        temp.add(DateFromDatetimeDescriptor.FACTORY);
        temp.add(TimeFromUnixTimeInMsDescriptor.FACTORY);
        temp.add(TimeFromDatetimeDescriptor.FACTORY);
        temp.add(DatetimeFromUnixTimeInMsDescriptor.FACTORY);
        temp.add(DatetimeFromUnixTimeInSecsDescriptor.FACTORY);
        temp.add(DatetimeFromDateAndTimeDescriptor.FACTORY);
        temp.add(CalendarDurationFromDateTimeDescriptor.FACTORY);
        temp.add(CalendarDuartionFromDateDescriptor.FACTORY);
        temp.add(AdjustDateTimeForTimeZoneDescriptor.FACTORY);
        temp.add(AdjustTimeForTimeZoneDescriptor.FACTORY);
        temp.add(IntervalBeforeDescriptor.FACTORY);
        temp.add(IntervalAfterDescriptor.FACTORY);
        temp.add(IntervalMeetsDescriptor.FACTORY);
        temp.add(IntervalMetByDescriptor.FACTORY);
        temp.add(IntervalOverlapsDescriptor.FACTORY);
        temp.add(IntervalOverlappedByDescriptor.FACTORY);
        temp.add(OverlapDescriptor.FACTORY);
        temp.add(IntervalStartsDescriptor.FACTORY);
        temp.add(IntervalStartedByDescriptor.FACTORY);
        temp.add(IntervalCoversDescriptor.FACTORY);
        temp.add(IntervalCoveredByDescriptor.FACTORY);
        temp.add(IntervalEndsDecriptor.FACTORY);
        temp.add(IntervalEndedByDescriptor.FACTORY);
        temp.add(CurrentDateDescriptor.FACTORY);
        temp.add(CurrentTimeDescriptor.FACTORY);
        temp.add(CurrentDateTimeDescriptor.FACTORY);
        temp.add(DurationFromMillisecondsDescriptor.FACTORY);
        temp.add(DurationFromMonthsDescriptor.FACTORY);
        temp.add(YearMonthDurationComparatorDecriptor.GREATER_THAN_FACTORY);
        temp.add(YearMonthDurationComparatorDecriptor.LESS_THAN_FACTORY);
        temp.add(DayTimeDurationComparatorDescriptor.GREATER_THAN_FACTORY);
        temp.add(DayTimeDurationComparatorDescriptor.LESS_THAN_FACTORY);
        temp.add(MonthsFromYearMonthDurationDescriptor.FACTORY);
        temp.add(MillisecondsFromDayTimeDurationDescriptor.FACTORY);
        temp.add(DurationEqualDescriptor.FACTORY);
        temp.add(GetYearMonthDurationDescriptor.FACTORY);
        temp.add(GetDayTimeDurationDescriptor.FACTORY);
        temp.add(IntervalBinDescriptor.FACTORY);
        temp.add(OverlapBinsDescriptor.FACTORY);
        temp.add(DayOfWeekDescriptor.FACTORY);
        temp.add(ParseDateDescriptor.FACTORY);
        temp.add(ParseTimeDescriptor.FACTORY);
        temp.add(ParseDateTimeDescriptor.FACTORY);
        temp.add(PrintDateDescriptor.FACTORY);
        temp.add(PrintTimeDescriptor.FACTORY);
        temp.add(PrintDateTimeDescriptor.FACTORY);
        temp.add(GetOverlappingIntervalDescriptor.FACTORY);
        temp.add(DurationFromIntervalDescriptor.FACTORY);

        // Interval constructor
        temp.add(AIntervalFromDateConstructorDescriptor.FACTORY);
        temp.add(AIntervalFromTimeConstructorDescriptor.FACTORY);
        temp.add(AIntervalFromDateTimeConstructorDescriptor.FACTORY);
        temp.add(AIntervalStartFromDateConstructorDescriptor.FACTORY);
        temp.add(AIntervalStartFromDateTimeConstructorDescriptor.FACTORY);
        temp.add(AIntervalStartFromTimeConstructorDescriptor.FACTORY);

        IFunctionManager mgr = new FunctionManagerImpl();
        for (IFunctionDescriptorFactory fdFactory : temp) {
            mgr.registerFunction(fdFactory);
        }
        FunctionManagerHolder.setFunctionManager(mgr);
    }

    @Override
    public IBinaryBooleanInspectorFactory getBinaryBooleanInspectorFactory() {
        return AqlBinaryBooleanInspectorImpl.FACTORY;
    }

    @Override
    public IBinaryComparatorFactoryProvider getBinaryComparatorFactoryProvider() {
        return AqlBinaryComparatorFactoryProvider.INSTANCE;
    }

    @Override
    public IBinaryHashFunctionFactoryProvider getBinaryHashFunctionFactoryProvider() {
        return AqlBinaryHashFunctionFactoryProvider.INSTANCE;
    }

    @Override
    public ISerializerDeserializerProvider getSerdeProvider() {
        return AqlSerializerDeserializerProvider.INSTANCE; // done
    }

    @Override
    public ITypeTraitProvider getTypeTraitProvider() {
        return AqlTypeTraitProvider.INSTANCE;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ICopyEvaluatorFactory getFieldAccessEvaluatorFactory(ARecordType recType, List<String> fldName,
            int recordColumn) throws AlgebricksException {
        String[] names = recType.getFieldNames();
        int n = names.length;
        boolean fieldFound = false;
        ICopyEvaluatorFactory recordEvalFactory = new ColumnAccessEvalFactory(recordColumn);
        ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        DataOutput dos = abvs.getDataOutput();
        ICopyEvaluatorFactory evalFactory = null;
        if (fldName.size() == 1) {
            for (int i = 0; i < n; i++) {
                if (names[i].equals(fldName.get(0))) {
                    fieldFound = true;
                    try {
                        AInt32 ai = new AInt32(i);
                        AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(ai.getType()).serialize(ai,
                                dos);
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                    ICopyEvaluatorFactory fldIndexEvalFactory = new ConstantEvalFactory(Arrays.copyOf(
                            abvs.getByteArray(), abvs.getLength()));

                    evalFactory = new FieldAccessByIndexEvalFactory(recordEvalFactory, fldIndexEvalFactory, recType);
                    return evalFactory;
                }
            }
        }
        if (fldName.size() > 1 || (!fieldFound && recType.isOpen())) {
            if (fldName.size() == 1) {
                AString as = new AString(fldName.get(0));
                try {
                    AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(as.getType()).serialize(as,
                            dos);
                } catch (HyracksDataException e) {
                    throw new AlgebricksException(e);
                }
            } else {
                AOrderedList as = new AOrderedList(fldName);
                try {
                    AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(as.getType()).serialize(as,
                            dos);
                } catch (HyracksDataException e) {
                    throw new AlgebricksException(e);
                }
            }
            ICopyEvaluatorFactory fldNameEvalFactory = new ConstantEvalFactory(Arrays.copyOf(abvs.getByteArray(),
                    abvs.getLength()));
            ICopyEvaluatorFactory[] factories = new ICopyEvaluatorFactory[2];
            factories[0] = recordEvalFactory;
            factories[1] = fldNameEvalFactory;
            if (fldName.size() > 1) {
                evalFactory = new FieldAccessNestedEvalFactory(recordEvalFactory, fldNameEvalFactory, recType, fldName);
            } else {
                evalFactory = FieldAccessByNameDescriptor.FACTORY.createFunctionDescriptor().createEvaluatorFactory(
                        factories);
            }
            return evalFactory;
        } else {
            throw new AlgebricksException("Could not find field " + fldName + " in the schema.");
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public ICopyEvaluatorFactory[] createMBRFactory(ARecordType recType, List<String> fldName, int recordColumn,
            int dimension, List<String> filterFieldName) throws AlgebricksException {
        ICopyEvaluatorFactory evalFactory = getFieldAccessEvaluatorFactory(recType, fldName, recordColumn);
        int numOfFields = dimension * 2;
        ICopyEvaluatorFactory[] evalFactories = new ICopyEvaluatorFactory[numOfFields
                + (filterFieldName == null ? 0 : 1)];

        ArrayBackedValueStorage abvs1 = new ArrayBackedValueStorage();
        DataOutput dos1 = abvs1.getDataOutput();
        try {
            AInt32 ai = new AInt32(dimension);
            AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(ai.getType()).serialize(ai, dos1);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
        ICopyEvaluatorFactory dimensionEvalFactory = new ConstantEvalFactory(Arrays.copyOf(abvs1.getByteArray(),
                abvs1.getLength()));

        for (int i = 0; i < numOfFields; i++) {
            ArrayBackedValueStorage abvs2 = new ArrayBackedValueStorage();
            DataOutput dos2 = abvs2.getDataOutput();
            try {
                AInt32 ai = new AInt32(i);
                AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(ai.getType()).serialize(ai, dos2);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
            }
            ICopyEvaluatorFactory coordinateEvalFactory = new ConstantEvalFactory(Arrays.copyOf(abvs2.getByteArray(),
                    abvs2.getLength()));

            evalFactories[i] = new CreateMBREvalFactory(evalFactory, dimensionEvalFactory, coordinateEvalFactory);
        }
        if (filterFieldName != null) {
            evalFactories[numOfFields] = getFieldAccessEvaluatorFactory(recType, filterFieldName, recordColumn);
        }
        return evalFactories;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Triple<ICopyEvaluatorFactory, ScalarFunctionCallExpression, IAType> partitioningEvaluatorFactory(
            ARecordType recType, List<String> fldName) throws AlgebricksException {
        String[] names = recType.getFieldNames();
        int n = names.length;
        if (fldName.size() > 1) {
            for (int i = 0; i < n; i++) {
                if (names[i].equals(fldName.get(0))) {
                    ICopyEvaluatorFactory recordEvalFactory = new ColumnAccessEvalFactory(
                            GlobalConfig.DEFAULT_INPUT_DATA_COLUMN);
                    ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
                    DataOutput dos = abvs.getDataOutput();
                    try {
                        AInt32 ai = new AInt32(i);
                        AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(ai.getType()).serialize(ai,
                                dos);
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                    ICopyEvaluatorFactory fldIndexEvalFactory = new ConstantEvalFactory(Arrays.copyOf(
                            abvs.getByteArray(), abvs.getLength()));
                    ICopyEvaluatorFactory evalFactory = new FieldAccessByIndexEvalFactory(recordEvalFactory,
                            fldIndexEvalFactory, recType);
                    IFunctionInfo finfoAccess = AsterixBuiltinFunctions
                            .getAsterixFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX);

                    ScalarFunctionCallExpression partitionFun = new ScalarFunctionCallExpression(finfoAccess,
                            new MutableObject<ILogicalExpression>(new VariableReferenceExpression(METADATA_DUMMY_VAR)),
                            new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                                    new AInt32(i)))));
                    return new Triple<ICopyEvaluatorFactory, ScalarFunctionCallExpression, IAType>(evalFactory,
                            partitionFun, recType.getFieldTypes()[i]);
                }
            }
        } else {
            ICopyEvaluatorFactory recordEvalFactory = new ColumnAccessEvalFactory(
                    GlobalConfig.DEFAULT_INPUT_DATA_COLUMN);
            ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
            DataOutput dos = abvs.getDataOutput();
            AOrderedList as = new AOrderedList(fldName);
            try {
                AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(as.getType()).serialize(as, dos);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
            }
            ICopyEvaluatorFactory fldNameEvalFactory = new ConstantEvalFactory(Arrays.copyOf(abvs.getByteArray(),
                    abvs.getLength()));
            ICopyEvaluatorFactory evalFactory = new FieldAccessNestedEvalFactory(recordEvalFactory, fldNameEvalFactory,
                    recType, fldName);
            IFunctionInfo finfoAccess = AsterixBuiltinFunctions
                    .getAsterixFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_NESTED);

            ScalarFunctionCallExpression partitionFun = new ScalarFunctionCallExpression(finfoAccess,
                    new MutableObject<ILogicalExpression>(new VariableReferenceExpression(METADATA_DUMMY_VAR)),
                    new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(as))));
            try {
                return new Triple<ICopyEvaluatorFactory, ScalarFunctionCallExpression, IAType>(evalFactory,
                        partitionFun, recType.getSubFieldType(fldName));
            } catch (IOException e) {
                throw new AlgebricksException(e);
            }
        }
        throw new AlgebricksException("Could not find field " + fldName + " in the schema.");
    }

    @Override
    public IFunctionDescriptor resolveFunction(ILogicalExpression expr, IVariableTypeEnvironment context)
            throws AlgebricksException {
        FunctionIdentifier fnId = ((AbstractFunctionCallExpression) expr).getFunctionIdentifier();
        IFunctionManager mgr = FunctionManagerHolder.getFunctionManager();
        IFunctionDescriptor fd = mgr.lookupFunction(fnId);
        if (fd == null) {
            throw new AsterixRuntimeException("Unresolved function " + fnId);
        }
        typeInference(expr, fd, context);
        return fd;
    }

    private void typeInference(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context)
            throws AlgebricksException {
        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.LISTIFY)) {
            AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
            if (f.getArguments().size() == 0) {
                ((ListifyAggregateDescriptor) fd).reset(new AOrderedListType(null, null));
            } else {
                IAType itemType = (IAType) context.getType(f.getArguments().get(0).getValue());
                if (itemType instanceof AUnionType) {
                    if (((AUnionType) itemType).isNullableType()) {
                        itemType = ((AUnionType) itemType).getUnionList().get(
                                AUnionType.OPTIONAL_TYPE_INDEX_IN_UNION_LIST);
                    } else {
                        // Convert UNION types into ANY.
                        itemType = BuiltinType.ANY;
                    }
                }
                ((ListifyAggregateDescriptor) fd).reset(new AOrderedListType(itemType, null));
            }
        }

        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.RECORD_MERGE)) {
            AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
            IAType outType = (IAType) context.getType(expr);
            IAType type0 = (IAType) context.getType(f.getArguments().get(0).getValue());
            IAType type1 = (IAType) context.getType(f.getArguments().get(1).getValue());
            ((RecordMergeDescriptor) fd).reset(outType, type0, type1);
        }

        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.ADD_FIELDS)) {
            AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
            IAType outType = (IAType) context.getType(expr);
            IAType type0 = (IAType) context.getType(f.getArguments().get(0).getValue());
            ILogicalExpression le = f.getArguments().get(1).getValue();
            IAType type1 = (IAType) context.getType(le);
            if (type0.getTypeTag().equals(ATypeTag.ANY)) {
                type0 = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
            }
            if (type1.getTypeTag().equals(ATypeTag.ANY)) {
                type1 = DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE;
            }
            ((RecordAddFieldsDescriptor) fd).reset(outType, type0, type1);
        }

        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.REMOVE_FIELDS)) {
            AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
            IAType outType = (IAType) context.getType(expr);
            IAType type0 = (IAType) context.getType(f.getArguments().get(0).getValue());
            ILogicalExpression le = f.getArguments().get(1).getValue();
            IAType type1 = (IAType) context.getType(le);
            if (type0.getTypeTag().equals(ATypeTag.ANY)) {
                type0 = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
            }
            if (type1.getTypeTag().equals(ATypeTag.ANY)) {
                type1 = DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE;
            }
            ((RecordRemoveFieldsDescriptor) fd).reset(outType, type0, type1);
        }

        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.CAST_RECORD)) {
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
            ARecordType rt = (ARecordType) TypeComputerUtilities.getRequiredType(funcExpr);
            IAType it = (IAType) context.getType(funcExpr.getArguments().get(0).getValue());
            if (it.getTypeTag().equals(ATypeTag.ANY)) {
                it = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
            }
            ((CastRecordDescriptor) fd).reset(rt, (ARecordType) it);
        }

        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.CAST_LIST)) {
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
            AbstractCollectionType rt = (AbstractCollectionType) TypeComputerUtilities.getRequiredType(funcExpr);
            IAType it = (IAType) context.getType(funcExpr.getArguments().get(0).getValue());
            if (it.getTypeTag().equals(ATypeTag.ANY)) {
                it = DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE;
            }
            ((CastListDescriptor) fd).reset(rt, (AbstractCollectionType) it);
        }

        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.FLOW_RECORD)) {
            ARecordType it = (ARecordType) TypeComputerUtilities.getInputType((AbstractFunctionCallExpression) expr);
            ((FlowRecordDescriptor) fd).reset(it);
        }

        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.OPEN_RECORD_CONSTRUCTOR)) {
            ARecordType rt = (ARecordType) context.getType(expr);
            ((OpenRecordConstructorDescriptor) fd).reset(rt, computeOpenFields((AbstractFunctionCallExpression) expr, rt));
        }

        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.CLOSED_RECORD_CONSTRUCTOR)) {
            ((ClosedRecordConstructorDescriptor) fd).reset((ARecordType) context.getType(expr));
        }

        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.ORDERED_LIST_CONSTRUCTOR)) {
            ((OrderedListConstructorDescriptor) fd).reset((AOrderedListType) context.getType(expr));
        }

        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.UNORDERED_LIST_CONSTRUCTOR)) {
            ((UnorderedListConstructorDescriptor) fd).reset((AUnorderedListType) context.getType(expr));
        }

        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX)) {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
            IAType t = (IAType) context.getType(fce.getArguments().get(0).getValue());
            switch (t.getTypeTag()) {
                case RECORD: {
                    ARecordType recType = (ARecordType) t;
                    ((FieldAccessByIndexDescriptor) fd).reset(recType);
                    break;
                }
                case UNION: {
                    AUnionType unionT = (AUnionType) t;
                    if (unionT.isNullableType()) {
                        IAType t2 = unionT.getUnionList().get(1);
                        if (t2.getTypeTag() == ATypeTag.RECORD) {
                            ARecordType recType = (ARecordType) t2;
                            ((FieldAccessByIndexDescriptor) fd).reset(recType);
                            break;
                        }
                    }
                    throw new NotImplementedException("field-access-by-index for data of type " + t);
                }
                default: {
                    throw new NotImplementedException("field-access-by-index for data of type " + t);
                }
            }
        }

        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.FIELD_ACCESS_NESTED)) {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
            IAType t = (IAType) context.getType(fce.getArguments().get(0).getValue());
            AOrderedList fieldPath = (AOrderedList) (((AsterixConstantValue) ((ConstantExpression) fce.getArguments()
                    .get(1).getValue()).getValue()).getObject());
            List<String> listFieldPath = new ArrayList<String>();
            for (int i = 0; i < fieldPath.size(); i++) {
                listFieldPath.add(((AString) fieldPath.getItem(i)).getStringValue());
            }

            switch (t.getTypeTag()) {
                case RECORD: {
                    ARecordType recType = (ARecordType) t;
                    ((FieldAccessNestedDescriptor) fd).reset(recType, listFieldPath);
                    break;
                }
                default: {
                    throw new NotImplementedException("field-access-nested for data of type " + t);
                }
            }
        }

    }

    /**
     *  A method necessary to extract a ordered list of paths from AQL expression
     *  That is, it will convert ["foo", ["foo2", "bar"]] to a nested orderelist object
     *
     *
     * @param expression
     *      The expression, normally as {@link AbstractFunctionCallExpression}
     * @param listType
     *      The type the list input
     * @param context
     *      The current type environment context
     * @return
     *      {@link AOrderedList}
     *
     * @throws AlgebricksException
     */
    public AOrderedList computePathLists(ILogicalExpression expression, IAType listType, IVariableTypeEnvironment context)
            throws AlgebricksException {
        AOrderedList pathAList = null;

        if (expression.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            pathAList = (AOrderedList) (((AsterixConstantValue) ((ConstantExpression) expression).getValue()).getObject());
        } else {
            AbstractFunctionCallExpression funcExp = (AbstractFunctionCallExpression) expression;
            List<Mutable<ILogicalExpression>> args = funcExp.getArguments();
            pathAList = new AOrderedList((AOrderedListType) listType);
            for (int i = 0; i < args.size(); i++) {
                ILogicalExpression exp = args.get(i).getValue();
                if (exp.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                    ConstantExpression ce = (ConstantExpression) exp;

                    if (!(ce.getValue() instanceof AsterixConstantValue)) {
                        throw new AlgebricksException("Expecting a list of strings and found " + ce.getValue()
                                + " instead.");
                    }
                    IAObject item = ((AsterixConstantValue) ce.getValue()).getObject();
                    pathAList.add(item);
                } else if (exp.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    List<Mutable<ILogicalExpression>> subFunctionArgs = ((AbstractFunctionCallExpression) exp)
                            .getArguments();
                    AOrderedList subPathOrderedList = new AOrderedList((AOrderedListType) context.getType(exp));
                    for (int j = 0; j < subFunctionArgs.size(); j++) {
                        ConstantExpression ce = (ConstantExpression) subFunctionArgs.get(j).getValue();
                        if (!(ce.getValue() instanceof AsterixConstantValue)) {
                            throw new AlgebricksException("Expecting a list of strings and found " + ce.getValue()
                                    + " instead.");
                        }
                        IAObject item = ((AsterixConstantValue) ce.getValue()).getObject();
                        subPathOrderedList.add(item);
                    }
                    pathAList.add(subPathOrderedList);

                }

            }
        }

        return pathAList;
    }

    private boolean[] computeOpenFields(AbstractFunctionCallExpression expr, ARecordType recType) {
        int n = expr.getArguments().size() / 2;
        boolean[] open = new boolean[n];
        for (int i = 0; i < n; i++) {
            Mutable<ILogicalExpression> argRef = expr.getArguments().get(2 * i);
            ILogicalExpression arg = argRef.getValue();
            if (arg.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                String fn = ((AString) ((AsterixConstantValue) ((ConstantExpression) arg).getValue()).getObject())
                        .getStringValue();
                open[i] = true;
                for (String s : recType.getFieldNames()) {
                    if (s.equals(fn)) {
                        open[i] = false;
                        break;
                    }
                }
            } else {
                open[i] = true;
            }
        }
        return open;
    }

    @Override
    public IPrinterFactoryProvider getPrinterFactoryProvider() {
        return AqlPrinterFactoryProvider.INSTANCE;
    }

    @Override
    public IPrinterFactoryProvider getJSONPrinterFactoryProvider() {
        return AqlJSONPrinterFactoryProvider.INSTANCE;
    }

    @Override
    public IPrinterFactoryProvider getCSVPrinterFactoryProvider() {
        return AqlCSVPrinterFactoryProvider.INSTANCE;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ICopyEvaluatorFactory getConstantEvalFactory(IAlgebricksConstantValue value) throws AlgebricksException {
        IAObject obj = null;
        if (value.isNull()) {
            obj = ANull.NULL;
        } else if (value.isTrue()) {
            obj = ABoolean.TRUE;
        } else if (value.isFalse()) {
            obj = ABoolean.FALSE;
        } else {
            AsterixConstantValue acv = (AsterixConstantValue) value;
            obj = acv.getObject();
        }
        ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        DataOutput dos = abvs.getDataOutput();
        try {
            AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(obj.getType()).serialize(obj, dos);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
        return new ConstantEvalFactory(Arrays.copyOf(abvs.getByteArray(), abvs.getLength()));
    }

    @Override
    public IBinaryIntegerInspectorFactory getBinaryIntegerInspectorFactory() {
        return AqlBinaryIntegerInspector.FACTORY;
    }

    @Override
    public ITupleParserFactory createTupleParser(ARecordType recType, IParseFileSplitsDecl decl) {
        return createTupleParser(recType, decl.isDelimitedFileFormat(), decl.getDelimChar(), decl.getQuote(),
                decl.getHasHeader());
    }

    @Override
    public ITupleParserFactory createTupleParser(ARecordType recType, boolean delimitedFormat, char delimiter,
            char quote, boolean hasHeader) {
        if (delimitedFormat) {
            int n = recType.getFieldTypes().length;
            IValueParserFactory[] fieldParserFactories = new IValueParserFactory[n];
            for (int i = 0; i < n; i++) {
                ATypeTag tag = recType.getFieldTypes()[i].getTypeTag();
                IValueParserFactory vpf = typeToValueParserFactMap.get(tag);
                if (vpf == null) {
                    throw new NotImplementedException("No value parser factory for delimited fields of type " + tag);
                }
                fieldParserFactories[i] = vpf;
            }
            return new NtDelimitedDataTupleParserFactory(recType, fieldParserFactories, delimiter, quote, hasHeader);
        } else {
            return new AdmSchemafullRecordParserFactory(recType);
        }
    }

    @Override
    public INullWriterFactory getNullWriterFactory() {
        return AqlNullWriterFactory.INSTANCE;
    }

    @Override
    public IExpressionEvalSizeComputer getExpressionEvalSizeComputer() {
        return new IExpressionEvalSizeComputer() {
            @Override
            public int getEvalSize(ILogicalExpression expr, IVariableEvalSizeEnvironment env)
                    throws AlgebricksException {
                switch (expr.getExpressionTag()) {
                    case CONSTANT: {
                        ConstantExpression c = (ConstantExpression) expr;
                        if (c == ConstantExpression.NULL) {
                            return 1;
                        } else if (c == ConstantExpression.FALSE || c == ConstantExpression.TRUE) {
                            return 2;
                        } else {
                            AsterixConstantValue acv = (AsterixConstantValue) c.getValue();
                            IAObject o = acv.getObject();
                            switch (o.getType().getTypeTag()) {
                                case DOUBLE: {
                                    return 9;
                                }
                                case BOOLEAN: {
                                    return 2;
                                }
                                case NULL: {
                                    return 1;
                                }
                                case INT32: {
                                    return 5;
                                }
                                case INT64: {
                                    return 9;
                                }
                                default: {
                                    // TODO
                                    return -1;
                                }
                            }
                        }
                    }
                    case FUNCTION_CALL: {
                        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
                        if (f.getFunctionIdentifier().equals(AsterixBuiltinFunctions.TID)) {
                            return 5;
                        } else {
                            // TODO
                            return -1;
                        }
                    }
                    default: {
                        // TODO
                        return -1;
                    }
                }
            }
        };
    }

    @Override
    public INormalizedKeyComputerFactoryProvider getNormalizedKeyComputerFactoryProvider() {
        return AqlNormalizedKeyComputerFactoryProvider.INSTANCE;
    }

    @Override
    public IBinaryHashFunctionFamilyProvider getBinaryHashFunctionFamilyProvider() {
        return AqlBinaryHashFunctionFamilyProvider.INSTANCE;
    }

    @Override
    public IPredicateEvaluatorFactoryProvider getPredicateEvaluatorFactoryProvider() {
        return AqlPredicateEvaluatorFactoryProvider.INSTANCE;
    }

}
