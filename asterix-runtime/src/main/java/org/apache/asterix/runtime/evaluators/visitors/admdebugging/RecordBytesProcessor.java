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

package org.apache.asterix.runtime.evaluators.visitors.admdebugging;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.runtime.RuntimeRecordTypeInfo;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.asterix.runtime.evaluators.functions.PointableUtils;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;

class RecordBytesProcessor {
    private final ByteArrayAccessibleOutputStream outputBos = new ByteArrayAccessibleOutputStream();
    private final DataOutputStream outputDos = new DataOutputStream(outputBos);

    private final Triple<IAType, RuntimeRecordTypeInfo, Long> nestedVisitorArg = new Triple<>(null,
            new RuntimeRecordTypeInfo(), 1L);

    private final RecordBuilder recordBuilder = new RecordBuilder();

    public void accessRecord(ARecordVisitablePointable pointable, AdmToBytesVisitor visitor, ARecordType requiredType,
            RuntimeRecordTypeInfo runtimeRecordTypeInfo, long nestedLevel, IVisitablePointable resultPointable)
            throws AsterixException {
        try {
            List<IVisitablePointable> fieldNames = pointable.getFieldNames();
            List<IVisitablePointable> fieldValues = pointable.getFieldValues();
            List<IVisitablePointable> fieldTypeTags = pointable.getFieldTypeTags();
            outputBos.reset();

            IAType reqFieldTypes[] = requiredType.getFieldTypes();
            recordBuilder.reset(requiredType);
            recordBuilder.init();

            for (int i = 0; i < fieldNames.size(); i++) {
                IVisitablePointable fieldValue = fieldValues.get(i);
                IVisitablePointable fieldName = fieldNames.get(i);
                ATypeTag fieldTypeTag = PointableUtils.getTypeTag(fieldTypeTags.get(i));

                int pos = runtimeRecordTypeInfo.getFieldIndex(fieldName.getByteArray(), fieldName.getStartOffset() + 1,
                        fieldName.getLength() - 1);

                if (pos >= 0 && fieldTypeTag != null) {
                    IAType reqfieldType = reqFieldTypes[pos];
                    if (NonTaggedFormatUtil.isOptional(reqFieldTypes[pos])) {
                        if (fieldTypeTags.get(pos) == null
                                || PointableUtils.sameType(ATypeTag.NULL, fieldTypeTags.get(pos))) {
                            reqfieldType = ((AUnionType) reqFieldTypes[pos]).getUnionList().get(0);
                        } else {
                            reqfieldType = ((AUnionType) reqFieldTypes[pos]).getNullableType();
                        }
                    }
                    nestedVisitorArg.first = reqfieldType;
                    if (reqfieldType.getTypeTag() == ATypeTag.RECORD) {
                        nestedVisitorArg.second.reset((ARecordType) reqfieldType);
                    }
                } else {
                    nestedVisitorArg.first = DefaultOpenFieldType.getDefaultOpenFieldType(fieldTypeTag);
                }
                nestedVisitorArg.third = nestedLevel + 1;
                IVisitablePointable tempFieldReference = fieldValue.accept(visitor, nestedVisitorArg);

                if (pos >= 0) {
                    recordBuilder.addField(pos, tempFieldReference);
                } else {
                    recordBuilder.addField(fieldNames.get(i), tempFieldReference);
                }
            }
            recordBuilder.write(outputDos, true);
            resultPointable.set(outputBos.getByteArray(), 0, outputBos.size());
        } catch (IOException e) {
            throw new AsterixException(e);
        }
    }
}
