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
package edu.uci.ics.asterix.external.library.adaptor;

import java.util.Map;

import edu.uci.ics.asterix.common.feeds.api.IDatasourceAdapter;
import edu.uci.ics.asterix.common.feeds.api.IIntakeProgressTracker;
import edu.uci.ics.asterix.external.adapter.factory.StreamBasedAdapterFactory;
import edu.uci.ics.asterix.metadata.feeds.IFeedAdapterFactory;
import edu.uci.ics.asterix.metadata.utils.AsterixTupleParserFactory;
import edu.uci.ics.asterix.metadata.utils.AsterixTupleParserFactory.InputDataFormat;
import edu.uci.ics.asterix.metadata.utils.IAsterixTupleParserFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class TweetGenAdaptorFactory extends StreamBasedAdapterFactory implements IFeedAdapterFactory {

    private static final long serialVersionUID = 1L;

    public static final String NAME = "tweetgen_adaptor";

    public static final String TWIITER_SERVER_HOST = "server";

    public static final String TWIITER_SERVER_PORT = "port";

    private ARecordType outputType;
    private Map<String, String> configuration;

    @Override
    public SupportedOperation getSupportedOperations() {
        return SupportedOperation.READ;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return new AlgebricksCountPartitionConstraint(1);
    }

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {
        IAsterixTupleParserFactory tupleParserFactory = new AsterixTupleParserFactory(configuration, outputType,
                getInputDataFormat());
        return new TweetGenAdaptor(tupleParserFactory, outputType, ctx, configuration, partition);
    }

    @Override
    public ARecordType getAdapterOutputType() {
        return outputType;
    }

    @Override
    public void configure(Map<String, String> configuration, ARecordType outputType) throws Exception {
        this.configuration = configuration;
        this.outputType = outputType;
        String host = configuration.get(TWIITER_SERVER_HOST);
        assert (host != null);
        int port = Integer.parseInt(configuration.get(TWIITER_SERVER_PORT));
        assert (port > 0);
    }

    @Override
    public boolean isRecordTrackingEnabled() {
        return true;
    }

    @Override
    public IIntakeProgressTracker createIntakeProgressTracker() {
        return new ProgressTracker();
    }

    private static class ProgressTracker implements IIntakeProgressTracker {

        private Map<String, String> configuration;

        @Override
        public void configure(Map<String, String> configuration) {
            this.configuration = configuration;
        }

        @Override
        public void notifyIngestedTupleTimestamp(long timestamp) {
            System.out.println(" LAST PERSISTED TUPLE TIMESTAMP " + timestamp);
        }

    }

    @Override
    public InputDataFormat getInputDataFormat() {
        return InputDataFormat.ADM;
    }
}
