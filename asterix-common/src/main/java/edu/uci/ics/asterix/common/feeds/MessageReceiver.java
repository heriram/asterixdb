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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class MessageReceiver<T> implements IMessageReceiver<T> {

    protected static final Logger LOGGER = Logger.getLogger(MessageReceiver.class.getName());

    protected final LinkedBlockingQueue<T> inbox;
    protected ExecutorService executor;

    public MessageReceiver() {
        inbox = new LinkedBlockingQueue<T>();
    }

    public MessageReceiver(LinkedBlockingQueue<T> inbox) {
        this.inbox = inbox;
    }

    public abstract void processMessage(T message) throws Exception;

    @Override
    public void start() {
        executor = Executors.newSingleThreadExecutor();
        executor.execute(new MessageReceiverRunnable<T>(this));
    }

    @Override
    public void sendMessage(T message) {
        inbox.add(message);
    }

    @Override
    public void close(boolean processPending) {
        if (executor != null) {
            executor.shutdown();
            executor = null;
            flushPendingMessages();
        }
    }

    private static class MessageReceiverRunnable<T> implements Runnable {

        private final LinkedBlockingQueue<T> inbox;
        private final MessageReceiver<T> messageReceiver;

        public MessageReceiverRunnable(MessageReceiver<T> messageReceiver) {
            this.inbox = messageReceiver.inbox;
            this.messageReceiver = messageReceiver;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    T message = inbox.take();
                    messageReceiver.processMessage(message);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (Exception e) {

                }
            }
        }
    }

    protected void flushPendingMessages() {
        while (!inbox.isEmpty()) {
            T message = null;
            try {
                message = inbox.take();
                processMessage(message);
            } catch (InterruptedException ie) {
                // ignore
            } catch (Exception e) {
                e.printStackTrace();
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Exception " + e + " in processing message " + message);
                }
            }
        }
    }

}
