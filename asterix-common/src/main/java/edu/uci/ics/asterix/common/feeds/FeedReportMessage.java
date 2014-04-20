package edu.uci.ics.asterix.common.feeds;

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.common.feeds.BasicFeedRuntime.FeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.api.IFeedMessage;
import edu.uci.ics.asterix.common.feeds.api.IFeedMetricCollector.ValueType;

/*
 * Copyright 2009-2014 by The Regents of the University of California
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

public class FeedReportMessage extends FeedMessage {

    private static final long serialVersionUID = 1L;

    private final FeedRuntimeId runtimeId;
    private final ValueType valueType;
    private int value;

    public FeedReportMessage(FeedRuntimeId runtimeId, ValueType valueType, int value) {
        super(MessageType.FEED_REPORT);
        this.runtimeId = runtimeId;
        this.valueType = valueType;
        this.value = value;
    }

    public void reset(int value) {
        this.value = value;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject obj = new JSONObject();
        obj.put(IFeedMessage.Constants.MESSAGE_TYPE, messageType.name());
        obj.put(IFeedMessage.Constants.DATAVERSE, runtimeId.getConnectionId().getFeedId().getDataverse());
        obj.put(IFeedMessage.Constants.FEED, runtimeId.getConnectionId().getFeedId().getFeedName());
        obj.put(IFeedMessage.Constants.DATASET, runtimeId.getConnectionId().getDatasetName());
        obj.put(IFeedMessage.Constants.RUNTIME_TYPE, runtimeId.getFeedRuntimeType());
        obj.put(IFeedMessage.Constants.PARTITION, runtimeId.getPartition());
        obj.put(IFeedMessage.Constants.VALUE_TYPE, valueType);
        obj.put(IFeedMessage.Constants.VALUE, value);
        return obj;
    }

}
