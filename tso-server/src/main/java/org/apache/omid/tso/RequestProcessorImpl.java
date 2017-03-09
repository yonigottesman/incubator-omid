/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.omid.tso;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.TimeoutHandler;
import com.lmax.disruptor.dsl.Disruptor;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.tso.TSOStateManager.TSOState;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static com.lmax.disruptor.dsl.ProducerType.MULTI;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.omid.tso.RequestProcessorImpl.RequestEvent.EVENT_FACTORY;

class RequestProcessorImpl implements EventHandler<RequestProcessorImpl.RequestEvent>, RequestProcessor, TimeoutHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RequestProcessorImpl.class);

    // Disruptor-related attributes
    private final ExecutorService disruptorExec;
    private final Disruptor<RequestEvent> disruptor;
    private final RingBuffer<RequestEvent> requestRing;

    private final TimestampOracle timestampOracle;
    private final CommitHashMap hashmap;
    private final MetricsRegistry metrics;
    private final PersistenceProcessor persistProc;
    private final ReplyProcessor replyProcessor;
    private long lowWatermark = -1L;

    @Inject
    RequestProcessorImpl(MetricsRegistry metrics,
                         TimestampOracle timestampOracle,
                         PersistenceProcessor persistProc,
                         Panicker panicker,
                         TSOServerConfig config, ReplyProcessor replyProcessor)
            throws IOException {


        // ------------------------------------------------------------------------------------------------------------
        // Disruptor initialization
        // ------------------------------------------------------------------------------------------------------------

        TimeoutBlockingWaitStrategy timeoutStrategy = new TimeoutBlockingWaitStrategy(config.getBatchPersistTimeoutInMs(), MILLISECONDS);

        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("request-%d").build();
        this.disruptorExec = Executors.newSingleThreadExecutor(threadFactory);

        this.disruptor = new Disruptor<>(EVENT_FACTORY, 1 << 12, disruptorExec, MULTI, timeoutStrategy);
        disruptor.handleExceptionsWith(new FatalExceptionHandler(panicker)); // This must be before handleEventsWith()
        disruptor.handleEventsWith(this);
        this.requestRing = disruptor.start();

        // ------------------------------------------------------------------------------------------------------------
        // Attribute initialization
        // ------------------------------------------------------------------------------------------------------------

        this.metrics = metrics;
        this.persistProc = persistProc;
        this.timestampOracle = timestampOracle;
        this.hashmap = new CommitHashMap(config.getConflictMapSize());
        this.replyProcessor = replyProcessor;
        LOG.info("RequestProcessor initialized");

    }

    /**
     * This should be called when the TSO gets leadership
     */
    @Override
    public void update(TSOState state) throws Exception {
        LOG.info("Initializing RequestProcessor state...");
        this.lowWatermark = state.getLowWatermark();
        persistProc.persistLowWatermark(lowWatermark).get(); // Sync persist
        LOG.info("RequestProcessor state initialized with LWMs {} and Epoch {}", lowWatermark, state.getEpoch());
    }

    @Override
    public void onEvent(RequestEvent event, long sequence, boolean endOfBatch) throws Exception {

        switch (event.getType()) {
            case TIMESTAMP:
                handleTimestamp(event);
                break;
            case COMMIT:
                handleCommit(event);
                break;
            default:
                throw new IllegalStateException("Event not allowed in Request Processor: " + event);
        }

    }

    @Override
    public void onTimeout(long sequence) throws Exception {

        // TODO We can not use this as a timeout trigger for flushing. This timeout is related to the time between
        // TODO (cont) arrivals of requests to the disruptor. We need another mechanism to trigger timeouts
        // TODO (cont) WARNING!!! Take care with the implementation because if there's other thread than request-0
        // TODO (cont) thread the one that calls persistProc.triggerCurrentBatchFlush(); we'll incur in concurrency issues
        // TODO (cont) This is because, in the current implementation, only the request-0 thread calls the public methods
        // TODO (cont) in persistProc and it is guaranteed that access them serially.
        persistProc.triggerCurrentBatchFlush();

    }

    @Override
    public void timestampRequest(Channel c, MonitoringContext monCtx) {

        monCtx.timerStart("request.processor.timestamp.latency");
        long seq = requestRing.next();
        RequestEvent e = requestRing.get(seq);
        RequestEvent.makeTimestampRequest(e, c, monCtx);
        requestRing.publish(seq);

    }

    @Override
    public void commitRequest(long startTimestamp, Collection<Long> writeSet, boolean isRetry, Channel c,
                              MonitoringContext monCtx) {

        monCtx.timerStart("request.processor.commit.latency");
        long seq = requestRing.next();
        RequestEvent e = requestRing.get(seq);
        RequestEvent.makeCommitRequest(e, startTimestamp, monCtx, writeSet, isRetry, c);
        requestRing.publish(seq);

    }

    private void handleTimestamp(RequestEvent requestEvent) throws Exception {

        long timestamp = timestampOracle.next();
        requestEvent.getMonCtx().timerStop("request.processor.timestamp.latency");
        //persistProc.addTimestampToBatch(timestamp, requestEvent.getChannel(), requestEvent.getMonCtx());
        replyProcessor.sendTimestampResponseLL(timestamp,requestEvent.getChannel());

    }

    private void handleCommit(RequestEvent event) throws Exception {

        long startTimestamp = event.getStartTimestamp();
        Iterable<Long> writeSet = event.writeSet();
        boolean isCommitRetry = event.isCommitRetry();
        Channel c = event.getChannel();

        boolean txCanCommit;

        int numCellsInWriteset = 0;
        // 0. check if it should abort
        if (startTimestamp <= lowWatermark) {
            txCanCommit = false;
        } else {
            // 1. check the write-write conflicts
            txCanCommit = true;
            for (long cellId : writeSet) {
                long value = hashmap.getLatestWriteForCell(cellId);
                if (value != 0 && value >= startTimestamp) {
                    txCanCommit = false;
                    break;
                }
                numCellsInWriteset++;
            }
        }

        if (txCanCommit) {
            // 2. commit

            long commitTimestamp = timestampOracle.next();

            if (numCellsInWriteset > 0) {
                long newLowWatermark = lowWatermark;

                for (long r : writeSet) {
                    long removed = hashmap.putLatestWriteForCell(r, commitTimestamp);
                    newLowWatermark = Math.max(removed, newLowWatermark);
                }

                if (newLowWatermark != lowWatermark) {
                    LOG.trace("Setting new low Watermark to {}", newLowWatermark);
                    lowWatermark = newLowWatermark;
                    persistProc.persistLowWatermark(newLowWatermark); // Async persist
                }
            }
            event.getMonCtx().timerStop("request.processor.commit.latency");
            //persistProc.addCommitToBatch(startTimestamp, commitTimestamp, c, event.getMonCtx());
            replyProcessor.sendCommitResponseLL(startTimestamp,commitTimestamp,event.getChannel());

        } else {

            event.getMonCtx().timerStop("request.processor.commit.latency");
            if (isCommitRetry) { // Re-check if it was already committed but the client retried due to a lag replying
                replyProcessor.sendAbortResponseLL(startTimestamp,event.getChannel());
                //persistProc.addCommitRetryToBatch(startTimestamp, c, event.getMonCtx());
            } else {
                replyProcessor.sendAbortResponseLL(startTimestamp,event.getChannel());
                //persistProc.addAbortToBatch(startTimestamp, c, event.getMonCtx());
            }

        }

    }

    @Override
    public void close() throws IOException {

        LOG.info("Terminating Request Processor...");
        disruptor.halt();
        disruptor.shutdown();
        LOG.info("\tRequest Processor Disruptor shutdown");
        disruptorExec.shutdownNow();
        try {
            disruptorExec.awaitTermination(3, SECONDS);
            LOG.info("\tRequest Processor Disruptor executor shutdown");
        } catch (InterruptedException e) {
            LOG.error("Interrupted whilst finishing Request Processor Disruptor executor");
            Thread.currentThread().interrupt();
        }
        LOG.info("Request Processor terminated");

    }

    final static class RequestEvent implements Iterable<Long> {

        enum Type {
            TIMESTAMP, COMMIT
        }

        private Type type = null;
        private Channel channel = null;

        private boolean isCommitRetry = false;
        private long startTimestamp = 0;
        private MonitoringContext monCtx;
        private long numCells = 0;

        private static final int MAX_INLINE = 40;
        private Long writeSet[] = new Long[MAX_INLINE];
        private Collection<Long> writeSetAsCollection = null; // for the case where there's more than MAX_INLINE

        static void makeTimestampRequest(RequestEvent e, Channel c, MonitoringContext monCtx) {
            e.type = Type.TIMESTAMP;
            e.channel = c;
            e.monCtx = monCtx;
        }

        static void makeCommitRequest(RequestEvent e,
                                      long startTimestamp,
                                      MonitoringContext monCtx,
                                      Collection<Long> writeSet,
                                      boolean isRetry,
                                      Channel c) {
            e.monCtx = monCtx;
            e.type = Type.COMMIT;
            e.channel = c;
            e.startTimestamp = startTimestamp;
            e.isCommitRetry = isRetry;
            if (writeSet.size() > MAX_INLINE) {
                e.numCells = writeSet.size();
                e.writeSetAsCollection = writeSet;
            } else {
                e.writeSetAsCollection = null;
                e.numCells = writeSet.size();
                int i = 0;
                for (Long cellId : writeSet) {
                    e.writeSet[i] = cellId;
                    i++;
                }
            }

        }

        MonitoringContext getMonCtx() {
            return monCtx;
        }

        Type getType() {
            return type;
        }

        long getStartTimestamp() {
            return startTimestamp;
        }

        Channel getChannel() {
            return channel;
        }

        @Override
        public Iterator<Long> iterator() {

            if (writeSetAsCollection != null) {
                return writeSetAsCollection.iterator();
            }

            return new Iterator<Long>() {
                int i = 0;

                @Override
                public boolean hasNext() {
                    return i < numCells;
                }

                @Override
                public Long next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    return writeSet[i++];
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };

        }

        Iterable<Long> writeSet() {

            return this;

        }

        boolean isCommitRetry() {
            return isCommitRetry;
        }

        final static EventFactory<RequestEvent> EVENT_FACTORY = new EventFactory<RequestEvent>() {
            @Override
            public RequestEvent newInstance() {
                return new RequestEvent();
            }
        };

    }

}
