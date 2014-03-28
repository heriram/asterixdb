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

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents a pool of reusable {@link DataBucket}
 */
public class DataBucketPool implements IFeedMemoryComponent {

    private static final Logger LOGGER = Logger.getLogger(DataBucketPool.class.getName());

    /** A unique identifier for the memory component **/
    private final int componentId;

    /** The {@link IFeedMemoryManager} for the NodeController **/
    private final IFeedMemoryManager memoryManager;

    /** A collection of available data buckets {@link DataBucket} **/
    private final List<DataBucket> pool;

    /** The total number of data buckets {@link DataBucket} allocated **/
    private int totalAllocation;

    private final int frameSize;

    public DataBucketPool(int componentId, IFeedMemoryManager memoryManager, int size, int frameSize) {
        this.componentId = componentId;
        this.memoryManager = memoryManager;
        this.pool = new ArrayList<DataBucket>();
        this.frameSize = frameSize;
        for (int i = 0; i < size; i++) {
            DataBucket bucket = new DataBucket(this);
            pool.add(bucket);
        }
        this.totalAllocation += size;
    }

    public synchronized void returnDataBucket(DataBucket bucket) {
        pool.add(bucket);
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("returned data bucket " + this + " back to the pool");
        }
    }

    public synchronized DataBucket getDataBucket() {
        if (pool.size() == 0) {
            if (!memoryManager.expandMemoryComponent(this)) {
                return null;
            }

        }
        return pool.remove(0);
    }

    @Override
    public Type getType() {
        return Type.POOL;
    }

    @Override
    public int getCurrentSize() {
        return totalAllocation;
    }

    @Override
    public int getComponentId() {
        return componentId;
    }

    @Override
    public void expand(int delta) {
        for (int i = 0; i < delta; i++) {
            DataBucket bucket = new DataBucket(this);
            pool.add(bucket);
        }
        totalAllocation += delta;
    }

    @Override
    public void reset() {
        totalAllocation -= pool.size();
        pool.clear();
    }

    @Override
    public String toString() {
        return "DataBucketPool" + "[" + componentId + "]" + "(" + totalAllocation + ")";
    }

    public int getSize() {
        return pool.size();
    }

    public int getFrameSize() {
        return frameSize;
    }

}