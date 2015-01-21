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
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.external.adapter.factory.NCFileSystemAdapterFactory;
import edu.uci.ics.asterix.external.dataset.adapter.NCFileSystemAdapter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.runtime.operators.file.AsterixTupleParserFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

/**
 * Adapter class extending {@link NCFileSystemAdapter}.
 * 
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

public class DirectoryBasedFileSystemBasedAdapter extends NCFileSystemAdapter {

    private static final long serialVersionUID = 1L;
    
    private FileSplit[] directorySplits;
    private String format;
    private List<File> fileList;


    public DirectoryBasedFileSystemBasedAdapter(ARecordType atype, Map<String, String> configuration,
            FileSplit[] dir_splits, ITupleParserFactory parser_factory,
            IHyracksTaskContext ctx) throws Exception {
        super(dir_splits, parser_factory, atype, ctx);
        directorySplits = dir_splits;
        this.format = configuration.get(AsterixTupleParserFactory.KEY_FORMAT);
    }

    @Override
    public InputStream getInputStream(int partition) throws IOException {
        return getFileContents(partition);
    }
    
    private void getFileList(int partition) throws IOException {
        File directory = directorySplits[partition].getLocalFile().getFile();
        if (directory == null) 
            throw new IllegalArgumentException("no path name provided");
        
        fileList = new ArrayList<File>();
        if (!directory.exists())
            throw new IOException("Invalid path or file");

        if (directory.isDirectory())
            fileList = Arrays.asList(directory.listFiles(new SupportedFileFilter(this.format)));
        else if (directory.isFile())
            fileList.add(directory);
    }
    
    /*
     * Perform a sequential read of the current file list
     */
    private InputStream getFileContents(int partition) throws IOException {
        getFileList(partition);
        Enumeration<InputStream> e = new Enumeration<InputStream>() {
            int index;

            @Override
            public boolean hasMoreElements() {
                return index < fileList.size();
            }

            @Override
            public InputStream nextElement() {
                index++;
                try {
                    return new FileInputStream(fileList.get(index - 1));
                } catch (FileNotFoundException ex) {
                    throw new RuntimeException("File not available!", ex);
                }
            }
        };
        return new SequenceInputStream(e);
        
    }

    public static class SupportedFileFilter implements FileFilter {
        private final String FORMAT_DELIMITTED_TEXT = AsterixTupleParserFactory.FORMAT_DELIMITED_TEXT;
        private final String FORMAT_ADM = AsterixTupleParserFactory.FORMAT_ADM;

        private String fileFormat;

        private SupportedFileFilter() {
            this.fileFormat = "adm";
        }

        private SupportedFileFilter(String fmt) {
            this.fileFormat = fmt;
        }

        @Override
        public boolean accept(File path) {
            String lcname = path.getName().toLowerCase();
            if (this.fileFormat.equals(FORMAT_ADM)) {
                return lcname.endsWith(".adm");
            } else if (this.fileFormat.equals(FORMAT_DELIMITTED_TEXT)) {
                return (lcname.endsWith(".csv") || lcname.endsWith(".csv") || lcname.endsWith(".txt") || lcname
                        .endsWith(".tbl"));
            } else
                // Unknown format
                return false;
        }
    }

}
