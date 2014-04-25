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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FeedFrameSpiller {

    private static final Logger LOGGER = Logger.getLogger(FeedFrameSpiller.class.getName());

    private final FeedConnectionId connectionId;
    private final FeedRuntimeId runtimeId;
    private final FeedPolicyAccessor policyAccessor;
    private BufferedOutputStream bos;
    private final int frameSize;
    private File file;
    private boolean fileCreated = false;
    private long bytesWritten = 0;
    private int spilledFrameCount = 0;

    public FeedFrameSpiller(FeedConnectionId connectionId, FeedRuntimeId runtimeId, int frameSize,
            FeedPolicyAccessor policyAccessor) throws IOException {
        this.connectionId = connectionId;
        this.runtimeId = runtimeId;
        this.policyAccessor = policyAccessor;
        this.frameSize = frameSize;
    }

    public boolean processMessage(ByteBuffer message) throws Exception {
        if (!fileCreated) {
            createFile();
            fileCreated = true;
        }
        long maxAllowed = policyAccessor.getMaxSpillOnDisk();
        if (maxAllowed != FeedPolicyAccessor.NO_LIMIT && bytesWritten + message.array().length > maxAllowed) {
            return false;
        } else {
            bos.write(message.array());
            bytesWritten += message.array().length;
            spilledFrameCount++;
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Spilled frame by " + runtimeId + " spill count " + spilledFrameCount);
            }
            return true;
        }
    }

    private void createFile() throws IOException {
        Date date = new Date();
        String dateSuffix = date.toString().replace(' ', '_');
        String fileName = connectionId.getFeedId() + "_" + connectionId.getDatasetName() + "_"
                + runtimeId.getFeedRuntimeType() + "_" + runtimeId.getPartition() + "_" + dateSuffix;

        file = new File(fileName);
        if (!file.exists()) {
            boolean success = file.createNewFile();
            if (!success) {
                throw new IOException("Unable to create spill file " + fileName + " for feed " + runtimeId);
            } else {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Created spill file " + file.getAbsolutePath());
                }
            }
        }
        bos = new BufferedOutputStream(new FileOutputStream(file));

    }

    public Iterator<ByteBuffer> replayData() throws Exception {
        bos.flush();
        return new FrameIterator(file.getName(), frameSize);
    }

    private static class FrameIterator implements Iterator<ByteBuffer> {

        private final BufferedInputStream bis;
        private final int frameSize;
        private int readFrameCount = 0;

        public FrameIterator(String filename, int frameSize) throws FileNotFoundException {
            bis = new BufferedInputStream(new FileInputStream(new File(filename)));
            this.frameSize = frameSize;
        }

        @Override
        public boolean hasNext() {
            boolean more = false;
            try {
                more = bis.available() > 0;
                if (!more) {
                    bis.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            return more;
        }

        @Override
        public ByteBuffer next() {
            ByteBuffer dataBuffer = null;
            try {
                dataBuffer = ByteBuffer.allocate(frameSize);
                Arrays.fill(dataBuffer.array(), (byte) 0);
                bis.read(dataBuffer.array(), 0, frameSize);
                readFrameCount++;
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Read spill frome " + readFrameCount);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return dataBuffer;
        }

        @Override
        public void remove() {
        }

    }

    public void reset() {
        bytesWritten = 0;
        //  file.delete();
        fileCreated = false;
        bos = null;
    }

    public void close() {
        if (bos != null) {
            try {
                bos.flush();
                bos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}