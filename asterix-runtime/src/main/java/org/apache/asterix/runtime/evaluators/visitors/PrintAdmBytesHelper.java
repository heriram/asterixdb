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

import org.apache.asterix.builders.AbvsBuilderFactory;
import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.ListBuilderFactory;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilderFactory;
import org.apache.asterix.om.pointables.nonvisitor.AListPointable;
import org.apache.asterix.om.pointables.nonvisitor.ARecordPointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class PrintAdmBytesHelper {
    private IObjectPool<IARecordBuilder, ATypeTag> recordBuilderPool = new ListObjectPool<IARecordBuilder, ATypeTag>(
            new RecordBuilderFactory());
    private IObjectPool<IAsterixListBuilder, ATypeTag> listBuilderPool = new ListObjectPool<IAsterixListBuilder, ATypeTag>(
            new ListBuilderFactory());
    private IObjectPool<IMutableValueStorage, ATypeTag> abvsBuilderPool = new ListObjectPool<IMutableValueStorage, ATypeTag>(
            new AbvsBuilderFactory());
    private IObjectPool<IPointable, ATypeTag> recordPointablePool = new ListObjectPool<IPointable, ATypeTag>(
            ARecordPointable.ALLOCATOR);
    private IObjectPool<IPointable, ATypeTag> listPointablePool = new ListObjectPool<IPointable, ATypeTag>(
            AListPointable.ALLOCATOR);

    public static PrintAdmBytesHelper getInstance() {
        return new PrintAdmBytesHelper();
    }

    private PrintAdmBytesHelper(){

    }

    public void reset() {
        abvsBuilderPool.reset();
    }

    public ArrayBackedValueStorage getTempBuffer() {
        return (ArrayBackedValueStorage) abvsBuilderPool.allocate(ATypeTag.BINARY);
    }

    public ARecordPointable getRecordPointable() {
        return (ARecordPointable) recordPointablePool.allocate(ATypeTag.RECORD);
    }

    public AListPointable getListPointable() {
        return (AListPointable) listPointablePool.allocate(ATypeTag.ORDEREDLIST);
    }

    public IARecordBuilder getRecordBuilder() {
        return recordBuilderPool.allocate(ATypeTag.RECORD);
    }

    public OrderedListBuilder getOrderedListBuilder() {
        return (OrderedListBuilder) listBuilderPool.allocate(ATypeTag.ORDEREDLIST);
    }
}
