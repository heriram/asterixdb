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

/**
 * A feed control message indicating the need to execute a give AQL.
 */
public class XAQLFeedMessage extends FeedMessage {

    private static final long serialVersionUID = 1L;

    private final String aql;
    private final FeedConnectionId connectionId;

    public XAQLFeedMessage(FeedConnectionId connectionId, String aql) {
        super(MessageType.XAQL, connectionId);
        this.connectionId = connectionId;
        this.aql = aql;
    }

    @Override
    public String toString() {
        return messageType.name() + feedConnectionId + " [" + aql + "] ";
    }

    public FeedConnectionId getConnectionId() {
        return connectionId;
    }

    public String getAql() {
        return aql;
    }

    @Override
    public String toJSON() throws JSONException {
        JSONObject obj = new JSONObject();
        obj.put("message-type", messageType.name());
        obj.put("dataverse", feedConnectionId.getFeedId().getDataverse());
        obj.put("feed", feedConnectionId.getFeedId().getFeedName());
        obj.put("dataset", feedConnectionId.getDatasetName());
        obj.put("aql", aql);
        return obj.toString();
    }

}
