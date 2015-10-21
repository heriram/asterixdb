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

package org.apache.asterix.runtime.evaluators.visitors;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.functions.PointableUtils;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;

class RecordPrintBytesAccessor {
    private final ByteArrayAccessibleOutputStream outputStream = new ByteArrayAccessibleOutputStream();
    private final DataInputStream dis = new DataInputStream(
            new ByteArrayInputStream(outputStream.getByteArray()));

    private PrintSerializedRecordVisitor visitor;

    public RecordPrintBytesAccessor() {
    }

    public String accessRecord(IVisitablePointable recAccessor, PrintSerializedRecordVisitor visitor, long maxLevel,
            Triple<IVisitablePointable, StringBuilder, Long> arg)
            throws AsterixException, IOException {

        ARecordVisitablePointable rec = ((ARecordVisitablePointable) recAccessor);
        List<IVisitablePointable> fieldNames = rec.getFieldNames();
        List<IVisitablePointable> fieldValues = rec.getFieldValues();
        List<IVisitablePointable> fieldTypeTags = rec.getFieldTypeTags();

        arg.second.append('{');

        for (int i=0; i<fieldNames.size(); i++) {
            ATypeTag typeTag = PointableUtils.getTypeTag(fieldTypeTags.get(i));
            arg.first = fieldNames.get(i);
            switch (typeTag){
                case RECORD:
                    arg.third++; // Increase the current level
                    if (arg.third <= maxLevel) {
                        ((ARecordVisitablePointable) fieldValues.get(i)).accept(visitor, arg);
                    } else {
                        arg.second.append("\"" + getFieldName(arg.first) + "\": ");
                        arg.second.append(printAnnotatedBytes(fieldValues.get(i)));
                    }
                    break;
                case ORDEREDLIST:
                case UNORDEREDLIST:
                    ((AListVisitablePointable)fieldValues.get(i)).accept(visitor, arg);
                    break;
                default:
                    ((AFlatValuePointable)fieldValues.get(i)).accept(visitor, arg);
            }
        }
        arg.second.append('}');
        return arg.second.toString();
    }

    public String getFieldName(IValueReference fieldNamePointable) throws IOException {
        outputStream.reset();
        outputStream.write(fieldNamePointable.getByteArray(),
                fieldNamePointable.getStartOffset() + 1, fieldNamePointable.getLength());
        dis.reset();

        return AStringSerializerDeserializer.INSTANCE.deserialize(dis).getStringValue();
    }

    public String printAnnotatedBytes(IVisitablePointable vp) {
        byte[] bytes = vp.getByteArray();
        int startOffset = vp.getStartOffset();
        int valueStartOffset = startOffset + getValueOffset(bytes[startOffset]);
        int len = vp.getLength() - valueStartOffset + startOffset; // value length

        StringBuilder sb = new StringBuilder("{ \"tag\":[");
        sb.append((int)bytes[startOffset] + "], ");
        extractLengthString(bytes, startOffset+1, valueStartOffset, sb);
        sb.append("\"value\": " +  byteArrayToString(bytes, valueStartOffset, len) + "} ");

        return sb.toString();
    }


    private String byteArrayToString(byte[] bytes, int offset, int length) {
        StringBuilder sb = new StringBuilder("[");
        sb.append(bytes[offset] & 0xff);
        int end = offset + length;
        for (int i=offset+1; i<end; i++) {
            sb.append(", ");
            sb.append(bytes[i] & 0xff);

        }
        sb.append(']');

        return sb.toString();
    }

    private void extractLengthString(byte[] vpBytes, int offset, int valueOffset, StringBuilder sb) {
        if (valueOffset>1) {
            sb.append("\"length\":");
            sb.append(byteArrayToString(vpBytes, offset, valueOffset - offset));
            sb.append(", ");
        }
    }

    // Get the relative value offset
    private int getValueOffset(byte typeByte) {
        int length=0;
        switch (EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(typeByte)) {
            case STRING:
            case UUID_STRING:
                return 3;
            case ORDEREDLIST:
            case UNORDEREDLIST:
                return 10;
            case RECORD:
            case SPARSERECORD:
                return 5;
            default:
                return 1;
        }
    }
}
