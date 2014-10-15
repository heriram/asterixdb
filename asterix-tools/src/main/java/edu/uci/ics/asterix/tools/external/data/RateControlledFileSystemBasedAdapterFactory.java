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
package edu.uci.ics.asterix.tools.external.data;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.api.IDatasourceAdapter;
import edu.uci.ics.asterix.common.feeds.api.IIntakeProgressTracker;
import edu.uci.ics.asterix.external.adapter.factory.HDFSAdapterFactory;
import edu.uci.ics.asterix.external.adapter.factory.NCFileSystemAdapterFactory;
import edu.uci.ics.asterix.external.adapter.factory.StreamBasedAdapterFactory;
import edu.uci.ics.asterix.external.dataset.adapter.FileSystemBasedAdapter;
import edu.uci.ics.asterix.metadata.feeds.IAdapterFactory;
import edu.uci.ics.asterix.metadata.feeds.IFeedAdapterFactory;
import edu.uci.ics.asterix.metadata.utils.AsterixTupleParserFactory;
import edu.uci.ics.asterix.metadata.utils.AsterixTupleParserFactory.InputDataFormat;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.runtime.operators.file.ADMDataParser;
import edu.uci.ics.asterix.runtime.operators.file.AbstractTupleParser;
import edu.uci.ics.asterix.runtime.operators.file.DelimitedDataParser;
import edu.uci.ics.asterix.runtime.operators.file.IDataParser;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

/**
 * Factory class for creating @see{RateControllerFileSystemBasedAdapter} The
 * adapter simulates a feed from the contents of a source file. The file can be
 * on the local file system or on HDFS. The feed ends when the content of the
 * source file has been ingested.
 */
public class RateControlledFileSystemBasedAdapterFactory extends StreamBasedAdapterFactory implements
        IFeedAdapterFactory {
    private static final long serialVersionUID = 1L;

    public static final String KEY_FILE_SYSTEM = "fs";
    public static final String LOCAL_FS = "localfs";
    public static final String HDFS = "hdfs";
    public static final String KEY_PATH = "path";
    public static final String KEY_FORMAT = "format";

    private IAdapterFactory adapterFactory;
    private String format;
    private ARecordType atype;

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {
        FileSystemBasedAdapter coreAdapter = (FileSystemBasedAdapter) adapterFactory.createAdapter(ctx, partition);
        return new RateControlledFileSystemBasedAdapter(atype, configuration, coreAdapter, format, parserFactory, ctx);
    }

    @Override
    public String getName() {
        return "file_feed";
    }

    private void checkRequiredArgs(Map<String, String> configuration) throws Exception {
        if (configuration.get(KEY_FILE_SYSTEM) == null) {
            throw new Exception("File system type not specified. (fs=?) File system could be 'localfs' or 'hdfs'");
        }
        if (configuration.get(IAdapterFactory.KEY_TYPE_NAME) == null) {
            throw new Exception("Record type not specified (type-name=?)");
        }
        if (configuration.get(KEY_PATH) == null) {
            throw new Exception("File path not specified (path=?)");
        }
        if (configuration.get(KEY_FORMAT) == null) {
            throw new Exception("File format not specified (format=?)");
        }
    }

    @Override
    public SupportedOperation getSupportedOperations() {
        return SupportedOperation.READ;
    }

    @Override
    public void configure(Map<String, String> configuration, ARecordType outputType) throws Exception {
        this.configuration = configuration;
        checkRequiredArgs(configuration);
        String fileSystem = (String) configuration.get(KEY_FILE_SYSTEM);
        String adapterFactoryClass = null;
        if (fileSystem.equalsIgnoreCase(LOCAL_FS)) {
            adapterFactoryClass = NCFileSystemAdapterFactory.class.getName();
        } else if (fileSystem.equals(HDFS)) {
            adapterFactoryClass = HDFSAdapterFactory.class.getName();
        } else {
            throw new AsterixException("Unsupported file system type " + fileSystem);
        }
        this.atype = outputType;
        format = configuration.get(KEY_FORMAT);
        adapterFactory = (IAdapterFactory) Class.forName(adapterFactoryClass).newInstance();
        adapterFactory.configure(configuration, outputType);
        configureFormat(outputType);
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return adapterFactory.getPartitionConstraint();
    }

    @Override
    public ARecordType getAdapterOutputType() {
        return atype;
    }

    @Override
    public InputDataFormat getInputDataFormat() {
        return InputDataFormat.UNKNOWN;
    }

    public boolean isRecordTrackingEnabled() {
        return false;
    }

    public IIntakeProgressTracker createIntakeProgressTracker() {
        throw new UnsupportedOperationException("Tracking of ingested records not enabled");
    }

}

class RateControlledTupleParserFactory implements ITupleParserFactory {

    private static final long serialVersionUID = 1L;

    private final ARecordType recordType;
    private final Map<String, String> configuration;
    private IValueParserFactory[] valueParserFactories;
    private char delimiter;
    private final ParserType parserType;

    public enum ParserType {
        ADM,
        DELIMITED_TEXT
    }

    public RateControlledTupleParserFactory(ARecordType recordType, IValueParserFactory[] valueParserFactories,
            char fieldDelimiter, Map<String, String> configuration) {
        this.recordType = recordType;
        this.valueParserFactories = valueParserFactories;
        this.delimiter = fieldDelimiter;
        this.configuration = configuration;
        this.parserType = ParserType.DELIMITED_TEXT;
    }

    public RateControlledTupleParserFactory(ARecordType recordType, Map<String, String> configuration) {
        this.recordType = recordType;
        this.configuration = configuration;
        this.parserType = ParserType.ADM;
    }

    @Override
    public ITupleParser createTupleParser(IHyracksTaskContext ctx) throws HyracksDataException {
        IDataParser dataParser = null;
        switch (parserType) {
            case ADM:
                dataParser = new ADMDataParser();
                break;
            case DELIMITED_TEXT:
                dataParser = new DelimitedDataParser(recordType, valueParserFactories, delimiter);
                break;
        }
        return new RateControlledTupleParser(ctx, recordType, dataParser, configuration);
    }

}

class RateControlledTupleParser extends AbstractTupleParser {

    private final IDataParser dataParser;
    private long interTupleInterval;
    private boolean delayConfigured;
    private boolean continueIngestion = true;

    public static final String INTER_TUPLE_INTERVAL = "tuple-interval";

    public RateControlledTupleParser(IHyracksTaskContext ctx, ARecordType recType, IDataParser dataParser,
            Map<String, String> configuration) throws HyracksDataException {
        super(ctx, recType);
        this.dataParser = dataParser;
        String propValue = configuration.get(INTER_TUPLE_INTERVAL);
        if (propValue != null) {
            interTupleInterval = Long.parseLong(propValue);
        } else {
            interTupleInterval = 0;
        }
        delayConfigured = interTupleInterval != 0;
    }

    public void setInterTupleInterval(long val) {
        this.interTupleInterval = val;
        this.delayConfigured = val > 0;
    }

    public void stop() {
        continueIngestion = false;
    }

    @Override
    public IDataParser getDataParser() {
        return dataParser;
    }

    @Override
    public void parse(InputStream in, IFrameWriter writer) throws HyracksDataException {

        appender.reset(frame, true);
        IDataParser parser = getDataParser();
        try {
            parser.initialize(in, recType, true);
            while (continueIngestion) {
                tb.reset();
                if (!parser.parse(tb.getDataOutput())) {
                    break;
                }
                tb.addFieldEndOffset();
                if (delayConfigured) {
                    Thread.sleep(interTupleInterval);
                }
                addTupleToFrame(writer);
            }
            if (appender.getTupleCount() > 0) {
                FrameUtils.flushFrame(frame, writer);
            }
        } catch (AsterixException ae) {
            throw new HyracksDataException(ae);
        } catch (IOException ioe) {
            throw new HyracksDataException(ioe);
        } catch (InterruptedException ie) {
            throw new HyracksDataException(ie);
        }
    }
}
