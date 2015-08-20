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
package edu.uci.ics.asterix.runtime.evaluators.functions;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.pointables.PointableAllocator;
import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.comparisons.DeepEqualAssessor;
import edu.uci.ics.asterix.runtime.evaluators.visitors.DeepEqualityVisitor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

import java.io.DataOutput;

public class DeepEqualityDescriptor  extends AbstractScalarFunctionDynamicDescriptor {
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new DeepEqualityDescriptor();
        }
    };

    private static final long serialVersionUID = 1L;
    private IAType inputType0;
    private IAType inputType1;

    public void reset(IAType inType0, IAType inType1) {
        this.inputType0 = inType0;
        this.inputType1 = inType1;
    }

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) {
        final ICopyEvaluatorFactory evalFactory0 = args[0];
        final ICopyEvaluatorFactory evalFactory1 = args[1];

        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;
            private final ISerializerDeserializer boolSerde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.ABOOLEAN);

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                final DataOutput out = output.getDataOutput();
                final ArrayBackedValueStorage abvs0 = new ArrayBackedValueStorage();
                final ICopyEvaluator eval0 = evalFactory0.createEvaluator(abvs0);

                final ArrayBackedValueStorage abvs1 = new ArrayBackedValueStorage();
                final ICopyEvaluator eval1 = evalFactory1.createEvaluator(abvs1);

                return new ICopyEvaluator() {
                    // pointable allocator
                    private PointableAllocator allocator = new PointableAllocator();
                    final IVisitablePointable accessor0 = allocator.allocateFieldValue(inputType0);
                    final IVisitablePointable accessor1 = allocator.allocateFieldValue(inputType1);

                    final DeepEqualityVisitor equalityVisitor = new DeepEqualityVisitor();
                    final Pair<IVisitablePointable, Boolean> arg = new
                            Pair<IVisitablePointable, Boolean>(accessor1, false);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        try {
                            abvs0.reset();
                            abvs1.reset();
                            eval0.evaluate(tuple);
                            eval1.evaluate(tuple);
                            accessor0.set(abvs0);
                            accessor1.set(abvs1);

                            // Using deep equality assessment to assess the equality of the two values
                            boolean isEqual = DeepEqualAssessor.INSTANCE.isEqual(accessor0, accessor1);
                            ABoolean result = isEqual ? ABoolean.TRUE : ABoolean.FALSE;

                            boolSerde.serialize(result, out);
                        } catch (Exception ioe) {
                            throw new AlgebricksException(ioe);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.DEEP_EQUAL;
    }
}
