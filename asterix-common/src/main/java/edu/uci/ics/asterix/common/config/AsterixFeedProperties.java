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
package edu.uci.ics.asterix.common.config;

public class AsterixFeedProperties extends AbstractAsterixProperties {

    private static final String FEED_CENTRAL_MANAGER_PORT_KEY = "feed.central.manager.port";
    private static final int FEED_CENTRAL_MANAGER_PORT_DEFAULT = 4500; // 64MB or 2048 frames (assuming 32768 as frame size)

    private static final String FEED_MEMORY_GLOBALBUDGET_KEY = "feed.memory.global.budget";
    private static final long FEED_MEMORY_GLOBALBUDGET_DEFAULT = 67108864; // 64MB or 2048 frames (assuming 32768 as frame size)

    private static final String FEED_MEMORY_AVAILABLE_WAIT_TIMEOUT_KEY = "feed.memory.available.wait.timeout";
    private static final long FEED_MEMORY_AVAILABLE_WAIT_TIMEOUT_DEFAULT = 10; // 10 seconds

    public AsterixFeedProperties(AsterixPropertiesAccessor accessor) {
        super(accessor);
    }

    public long getMemoryComponentGlobalBudget() {
        return accessor.getProperty(FEED_MEMORY_GLOBALBUDGET_KEY, FEED_MEMORY_GLOBALBUDGET_DEFAULT,
                PropertyInterpreters.getLongPropertyInterpreter());
    }

    public long getMemoryAvailableWaitTimeout() {
        return accessor.getProperty(FEED_MEMORY_AVAILABLE_WAIT_TIMEOUT_KEY, FEED_MEMORY_AVAILABLE_WAIT_TIMEOUT_DEFAULT,
                PropertyInterpreters.getLongPropertyInterpreter());
    }

    public int getFeedCentralManagerPort() {
        return accessor.getProperty(FEED_CENTRAL_MANAGER_PORT_KEY, FEED_CENTRAL_MANAGER_PORT_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

}