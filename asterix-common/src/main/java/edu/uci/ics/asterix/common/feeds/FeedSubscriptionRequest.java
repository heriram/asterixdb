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

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import edu.uci.ics.asterix.common.feeds.IFeedLifecycleListener.SubscriptionLocation;

/**
 * A request for subscribing to a feed.
 */
public class FeedSubscriptionRequest {

    /** Represents the feed point on the feed pipeline that serves as the source for this subscription **/
    private final FeedJointKey feedJointKey;

    /** Represents the subscription location in the source feed pipeline from where feed tuples are received. **/
    private final SubscriptionLocation subscriptionLocation;

    /** The list of functions that need to be applied in sequence after the data hand-off at the source feedPointKey. **/
    private final List<String> functionsToApply;

    /** The status associated with the subscription. */
    private SubscriptionStatus subscriptionStatus;

    /** The name of the policy that governs feed ingestion **/
    private final String policy;

    /** The policy associated with a feed connection. **/
    private final Map<String, String> policyParameters;

    /** The target dataset that would receive the feed. **/
    private final String targetDataset;

    private final FeedId subscribingFeedId;

    public enum SubscriptionStatus {
        INITIALIZED, // initial state upon creating a subscription request
        ACTIVE, // subscribing feed receives data 
        INACTIVE, // subscribing feed does not receive data 
        FAILED // subscription failed
    }

    public FeedSubscriptionRequest(FeedJointKey feedPointKey, SubscriptionLocation subscriptionLocation,
            List<String> functionsToApply, String targetDataset, String policy, Map<String, String> policyParameters,
            FeedId subscribingFeedId) {
        this.feedJointKey = feedPointKey;
        this.subscriptionLocation = subscriptionLocation;
        this.functionsToApply = functionsToApply;
        this.targetDataset = targetDataset;
        this.policy = policy;
        this.policyParameters = policyParameters;
        this.subscribingFeedId = subscribingFeedId;
        this.subscriptionStatus = SubscriptionStatus.INITIALIZED;
    }

    public FeedJointKey getFeedJointKey() {
        return feedJointKey;
    }

    public SubscriptionStatus getSubscriptionStatus() {
        return subscriptionStatus;
    }

    public void setSubscriptionStatus(SubscriptionStatus subscriptionStatus) {
        this.subscriptionStatus = subscriptionStatus;
    }

    public String getPolicy() {
        return policy;
    }

    public String getTargetDataset() {
        return targetDataset;
    }

    public SubscriptionLocation getSubscriptionLocation() {
        return subscriptionLocation;
    }

    public FeedId getSubscribingFeedId() {
        return subscribingFeedId;
    }

    public Map<String, String> getPolicyParameters() {
        return policyParameters;
    }

    public List<String> getFunctionsToApply() {
        return functionsToApply;
    }

    @Override
    public String toString() {
        return "Feed Subscription Request " + feedJointKey + " [" + subscriptionLocation + "]" + " Apply ("
                + StringUtils.join(functionsToApply, ",") + ")";
    }

}
