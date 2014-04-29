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
package edu.uci.ics.asterix.metadata.feeds;

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedMessage;
import edu.uci.ics.asterix.common.feeds.api.IFeedMessage;

/**
 * A feed control message indicating the need to end the feed. This message is dispatched
 * to all locations that host an operator involved in the feed pipeline.
 */
public class PrepareStallMessage extends FeedMessage {

    private static final long serialVersionUID = 1L;

    private final FeedConnectionId connectionId;

    public PrepareStallMessage(FeedConnectionId connectionId) {
        super(MessageType.PREPARE_STALL);
        this.connectionId = connectionId;
    }

    @Override
    public String toString() {
        return MessageType.PREPARE_STALL.name() + "  " + connectionId;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject obj = new JSONObject();
        obj.put(IFeedMessage.Constants.MESSAGE_TYPE, messageType.name());
        obj.put(IFeedMessage.Constants.DATAVERSE, connectionId.getFeedId().getDataverse());
        obj.put(IFeedMessage.Constants.FEED, connectionId.getFeedId().getFeedName());
        obj.put(IFeedMessage.Constants.DATASET, connectionId.getDatasetName());
        return obj;
    }

    public FeedConnectionId getConnectionId() {
        return connectionId;
    }

}
