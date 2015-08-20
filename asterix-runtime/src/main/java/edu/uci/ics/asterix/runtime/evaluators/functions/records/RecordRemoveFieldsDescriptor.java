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
package edu.uci.ics.asterix.runtime.evaluators.functions.records;

import edu.uci.ics.asterix.om.base.AOrderedList;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;

import java.util.List;

public class RecordRemoveFieldsDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    private static final byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    private static final byte SER_ORDEREDLIST_TYPE_TAG = ATypeTag.ORDEREDLIST.serialize();

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new RecordRemoveFieldsDescriptor();
        }
    };

    private  RecordRemoveFieldsDescriptor() {
    }

    private ARecordType outputRecordType;
    private ARecordType inputRecType;
    private AOrderedListType inputListType;
    private AOrderedList pathAList;
    private List<List<String>> pathList;

    public void reset(IAType outType, IAType inType, List<List<String>> pathList) {
        this.outputRecordType = (ARecordType) outType;
        this.inputRecType = (ARecordType) inType;
        this.pathList = pathList;
    }

    public void reset(IAType outType, IAType inType, IAType inListType) {
        outputRecordType = (ARecordType) outType;
        inputRecType = (ARecordType) inType;
        inputListType = (AOrderedListType) inListType;
    }

    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new RecordRemoveFieldsEvalFactory(args[0], args[1], outputRecordType, inputRecType, inputListType);
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.REMOVE_FIELDS;
    }

}