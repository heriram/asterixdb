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

import java.io.File;
import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.api.IDatasourceAdapter;
import edu.uci.ics.asterix.external.adapter.factory.NCFileSystemAdapterFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.operators.file.AsterixTupleParserFactory;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;

/**
 * Factory class extending {@link NCFileSystemAdapterFactory} for creating an instance of {@link DirectoryBasedFileSystemBasedAdapter}.
 * As with the NCFileSystemAdapter, a DirectoryBasedFileSystemBasedAdapter reads external data residing on the local file system of
 * an NC. The difference is that this adapter takes on the directory name as input and reads the data from all files - specified by
 * its format. E.g., if format=adm then it will load all files in the directory with extension ".adm".<br />
 * 
 * Usage:
 * 
 * <pre>
 * load dataset StreamDocuments using derectory_file_feed
 *         (("path"="127.0.0.1://full/directory/path/"),("format"="adm"));
 * </pre>
 */

public class DirectoryBasedFileSystemBasedAdapterFactory extends NCFileSystemAdapterFactory {

    private static final long serialVersionUID = 1L;

    private String KEY_PATH=AsterixTupleParserFactory.KEY_PATH;
    
    private ARecordType outputType;
 
    private FileSplit[] directorySplits;

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {        
         return new DirectoryBasedFileSystemBasedAdapter(outputType, configuration, directorySplits, parserFactory, ctx);
    }

    @Override
    public void configure(Map<String, String> configuration, ARecordType outputType) throws Exception {
        this.configuration = configuration;
        this.outputType = outputType;
        String[] splits = ((String) configuration.get(KEY_PATH)).split(",");
        IAType sourceDatatype = (IAType) outputType;
        configureDirectorySplits(splits);
        configureFormat(sourceDatatype);

    }
    
    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        String[] locations = new String[directorySplits.length];
        String location;
        for (int i = 0; i < directorySplits.length; i++) {
            location = getNodeResolver().resolveNode(directorySplits[i].getNodeName());
            locations[i] = location;
        }
        return new AlgebricksAbsolutePartitionConstraint(locations);

    }
        
    private void configureDirectorySplits(String[] splits) throws AsterixException {
        if (directorySplits == null) {
            directorySplits = new FileSplit[splits.length];
            String nodeName;
            String nodeLocalDirectoryPath;
            int count = 0;
            String trimmedValue;
            for (String splitPath : splits) {
                trimmedValue = splitPath.trim();
                if (!trimmedValue.contains("://")) {
                    throw new AsterixException("Invalid path: " + splitPath
                            + "\nUsage- path=\"Host://Absolute Directory Path\"");
                }
                nodeName = trimmedValue.split(":")[0];
                nodeLocalDirectoryPath = trimmedValue.split("://")[1];
                FileSplit dirSplit = new FileSplit(nodeName, new FileReference(new File(nodeLocalDirectoryPath)));
                directorySplits[count++] = dirSplit;
            }
        }
    }

    @Override
    public String getName() {
        return "directory_file_feed";
    }
}
