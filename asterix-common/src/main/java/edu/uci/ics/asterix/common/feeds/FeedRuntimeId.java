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
package edu.uci.ics.asterix.common.feeds;

import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;

public class FeedRuntimeId{

    public static final String DEFAULT_OPERAND_ID = "N/A";

    private final FeedRuntimeType runtimeType;
    private final int partition;
    private final String operandId;

    public FeedRuntimeId(FeedRuntimeType runtimeType, int partition, String operandId) {
        this.runtimeType = runtimeType;
        this.partition = partition;
        this.operandId = operandId;
    }

    @Override
    public String toString() {
        return runtimeType + "[" + partition + "]" + "{" + operandId + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FeedRuntimeId)) {
            return false;
        }
        FeedRuntimeId other = (FeedRuntimeId) o;
        return (other.getFeedRuntimeType().equals(runtimeType) && other.getOperandId().equals(operandId) && other
                .getPartition() == partition);
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    public FeedRuntimeType getFeedRuntimeType() {
        return runtimeType;
    }

    public int getPartition() {
        return partition;
    }

    public FeedRuntimeType getRuntimeType() {
        return runtimeType;
    }

    public String getOperandId() {
        return operandId;
    }

}