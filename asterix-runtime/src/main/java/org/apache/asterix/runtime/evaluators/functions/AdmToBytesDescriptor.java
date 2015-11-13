/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.runtime.evaluators.functions;

import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.typecomputer.impl.TypeComputerUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;

public class AdmToBytesDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    private ARecordType outRecType;
    private IAType inputType;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new AdmToBytesDescriptor();
        }
    };


    public void reset(IAType outType, IAType inType, IAType inputLevelType) {
        outRecType = TypeComputerUtils.extractRecordType(outType);

        switch (inType.getTypeTag()) {
            case RECORD:
                this.inputType = TypeComputerUtils.extractRecordType(inType);
                break;
            case UNORDEREDLIST:
                this.inputType = TypeComputerUtils.extractUnorderedListType(inType);
                break;
            case ORDEREDLIST:
                this.inputType = TypeComputerUtils.extractOrderedListType(inType);
                break;
            default:
                this.inputType = inType;
        }
    }


    private AdmToBytesDescriptor() {

    }

    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new AdmToBytesFactory(args[0], args[1], inputType, outRecType);
    }

    @Override public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.ADM_TO_BYTES;
    }
}
