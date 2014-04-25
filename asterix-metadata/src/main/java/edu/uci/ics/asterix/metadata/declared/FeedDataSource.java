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
package edu.uci.ics.asterix.metadata.declared;

import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.api.IFeedLifecycleListener.SubscriptionLocation;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.Feed;
import edu.uci.ics.asterix.metadata.entities.Feed.FeedType;
import edu.uci.ics.asterix.metadata.entities.InternalDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.NodeGroup;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.INodeDomain;

public class FeedDataSource extends AqlDataSource {

    private static final int DEFAULT_COMPUTE_CARDINALITY = 1;

    private Feed feed;
    private FeedId sourceFeedId;
    private final FeedType sourceFeedType;
    private final SubscriptionLocation location;
    private final String targetDataset;
    private final String[] locations;
    private int computeCardinality;

    public FeedDataSource(AqlSourceId id, String targetDataset, IAType itemType, AqlDataSourceType dataSourceType,
            FeedId sourceFeedId, FeedType sourceFeedType, SubscriptionLocation location, String[] locations)
            throws AlgebricksException {
        super(id, id.getDataverseName(), id.getDatasourceName(), itemType, dataSourceType);
        this.targetDataset = targetDataset;
        this.sourceFeedId = sourceFeedId;
        this.sourceFeedType = sourceFeedType;
        this.location = location;
        this.locations = locations;
        MetadataTransactionContext ctx = null;
        try {
            MetadataManager.INSTANCE.acquireReadLatch();
            ctx = MetadataManager.INSTANCE.beginTransaction();
            this.feed = MetadataManager.INSTANCE.getFeed(ctx, id.getDataverseName(), id.getDatasourceName());
            Dataset dataset = MetadataManager.INSTANCE.getDataset(ctx, id.getDataverseName(), targetDataset);
            computeCardinality = DEFAULT_COMPUTE_CARDINALITY;
            if (dataset != null) {
                InternalDatasetDetails idd = (InternalDatasetDetails) dataset.getDatasetDetails();
                String nodeGroup = idd.getNodeGroupName();
                NodeGroup ng = MetadataManager.INSTANCE.getNodegroup(ctx, nodeGroup);
                if (ng != null) {
                    computeCardinality = ng.getNodeNames().size();
                }
            }
            MetadataManager.INSTANCE.commitTransaction(ctx);
            initFeedDataSource(itemType);
        } catch (Exception e) {
            if (ctx != null) {
                try {
                    MetadataManager.INSTANCE.abortTransaction(ctx);
                } catch (Exception e2) {
                    e2.addSuppressed(e);
                    throw new IllegalStateException("Unable to abort " + e2.getMessage());
                }
            }

        } finally {
            MetadataManager.INSTANCE.releaseReadLatch();
        }
    }

    public Feed getFeed() {
        return feed;
    }

    @Override
    public IAType[] getSchemaTypes() {
        return schemaTypes;
    }

    @Override
    public INodeDomain getDomain() {
        return domain;
    }

    public String getTargetDataset() {
        return targetDataset;
    }

    public FeedId getSourceFeedId() {
        return sourceFeedId;
    }

    public SubscriptionLocation getLocation() {
        return location;
    }

    public String[] getLocations() {
        return locations;
    }

    private void initFeedDataSource(IAType itemType) {
        schemaTypes = new IAType[1];
        schemaTypes[0] = itemType;
        INodeDomain domainForExternalData = new INodeDomain() {
            @Override
            public Integer cardinality() {
                return null;
            }

            @Override
            public boolean sameAs(INodeDomain domain) {
                return domain == this;
            }
        };
        domain = domainForExternalData;
    }

    public FeedType getSourceFeedType() {
        return sourceFeedType;
    }

    public int getComputeCardinality() {
        return computeCardinality;
    }
}
