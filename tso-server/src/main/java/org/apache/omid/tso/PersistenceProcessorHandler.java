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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.lmax.disruptor.WorkHandler;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.metrics.Histogram;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.codahale.metrics.MetricRegistry.name;
import static org.apache.omid.tso.PersistEvent.Type.*;

public class PersistenceProcessorHandler implements WorkHandler<PersistenceProcessorImpl.PersistBatchEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(PersistenceProcessorHandler.class);

    @VisibleForTesting
    static final AtomicInteger consecutiveSequenceCreator = new AtomicInteger(0);

    private final String id;

    private final String tsoHostAndPort;
    private final LeaseManagement leaseManager;

    private final ReplyProcessor replyProcessor;
    private final RetryProcessor retryProcessor;
    private final CommitTable.Writer writer;
    final Panicker panicker;

    // Metrics in this component
    private final Timer flushTimer;
    private final Histogram batchSizeHistogram;
    private final Histogram flushedCommitEventsHistogram;

    @Inject
    PersistenceProcessorHandler(MetricsRegistry metrics,
                                String tsoHostAndPort, // TODO This should not be passed here. Should be part of panicker
                                LeaseManagement leaseManager,
                                CommitTable commitTable,
                                ReplyProcessor replyProcessor,
                                RetryProcessor retryProcessor,
                                Panicker panicker)
    throws InterruptedException, ExecutionException, IOException {

        this.id = String.valueOf(consecutiveSequenceCreator.getAndIncrement());
        this.tsoHostAndPort = tsoHostAndPort;
        this.leaseManager = leaseManager;
        this.writer = commitTable.getWriter();
        this.replyProcessor = replyProcessor;
        this.retryProcessor = retryProcessor;
        this.panicker = panicker;

        // Metrics setup
        String flushTimerName = name("tso", "persistence-processor-handler", id, "flush", "latency");
        flushTimer = metrics.timer(flushTimerName);
        String flushedCommitEventsName = name("tso", "persistence-processor-handler", id, "flushed", "commits", "size");
        flushedCommitEventsHistogram = metrics.histogram(flushedCommitEventsName);
        String batchSizeMetricsName = name("tso", "persistence-processor-handler", id, "batch", "size");
        batchSizeHistogram = metrics.histogram(batchSizeMetricsName);

    }

    public String getId() {
        return id;
    }

    @Override
    public void onEvent(PersistenceProcessorImpl.PersistBatchEvent batchEvent) throws Exception {

        int commitEventsToFlush = 0;
        Batch batch = batchEvent.getBatch();
        int numOfBatchedEvents = batch.getNumEvents();
        batchSizeHistogram.update(numOfBatchedEvents);
        for (int i=0; i < numOfBatchedEvents; i++) {
            PersistEvent event = batch.get(i);
            switch (event.getType()) {
                case TIMESTAMP:
                    event.getMonCtx().timerStop("persistence.processor.timestamp.latency");
                    break;
                case COMMIT:
                    writer.addCommittedTransaction(event.getStartTimestamp(), event.getCommitTimestamp());
                    commitEventsToFlush++;
                    break;
                case COMMIT_RETRY:
                    event.getMonCtx().timerStop("persistence.processor.commit-retry.latency");
                    break;
                case ABORT:
                    event.getMonCtx().timerStop("persistence.processor.abort.latency");
                    break;
                default:
                    throw new IllegalStateException("Event not allowed in Persistent Processor Handler: " + event);
            }
        }

        // Flush and send the responses back to the client. WARNING: Before sending the responses, first we need
        // to filter commit retries in the batch to disambiguate them.
        flush(commitEventsToFlush);
        filterAndDissambiguateClientRetries(batch);
        for (int i=0; i < batch.getNumEvents(); i++) { // Just for statistics
            PersistEvent event = batch.get(i);
            switch (event.getType()) {
                case TIMESTAMP:
                    event.getMonCtx().timerStart("reply.processor.timestamp.latency");
                    break;
                case COMMIT:
                    event.getMonCtx().timerStop("persistence.processor.commit.latency");
                    event.getMonCtx().timerStart("reply.processor.commit.latency");
                    break;
                case COMMIT_RETRY:
                    throw new IllegalStateException("COMMIT_RETRY events must be filtered before this step: " + event);
                case ABORT:
                    event.getMonCtx().timerStart("reply.processor.abort.latency");
                    break;
                default:
                    throw new IllegalStateException("Event not allowed in Persistent Processor Handler: " + event);
            }
        }
        replyProcessor.manageResponsesBatch(batchEvent.getBatchSequence(), batch);

    }

    void flush(int commitEventsToFlush) {

        commitSuicideIfNotMaster();
        try {
            long startFlushTimeInNs = System.nanoTime();
            if(commitEventsToFlush > 0) {
                writer.flush();
            }
            flushTimer.update(System.nanoTime() - startFlushTimeInNs);
            flushedCommitEventsHistogram.update(commitEventsToFlush);
        } catch (IOException e) {
            panicker.panic("Error persisting commit batch", e);
        }
        commitSuicideIfNotMaster();

    }

    private void commitSuicideIfNotMaster() {
        if (!leaseManager.stillInLeasePeriod()) {
            panicker.panic("Replica " + tsoHostAndPort + " lost mastership whilst flushing data. Committing suicide");
        }
    }

    void filterAndDissambiguateClientRetries(Batch batch) {

        int currentEventIdx = 0;
        while (currentEventIdx <= batch.getLastEventIdx()) {
            PersistEvent event = batch.get(currentEventIdx);
            if (event.getType() == COMMIT_RETRY) {
                retryProcessor.disambiguateRetryRequestHeuristically(event.getStartTimestamp(), event.getChannel(), event.getMonCtx());
                // Swap the disambiguated event with the last batch event & decrease the # of remaining elems to process
                swapBatchElements(batch, currentEventIdx, batch.getLastEventIdx());
                batch.decreaseNumEvents();
                if (batch.isEmpty()) {
                    break; // We're OK to call now the reply processor
                } else {
                    continue; // Otherwise we continue checking for retries from the new event in the current position
                }
            } else {
                currentEventIdx++; // Let's check if the next event was a retry
            }
        }

    }

    private void swapBatchElements(Batch batch, int firstIdx, int lastIdx) {
        PersistEvent tmpEvent = batch.get(firstIdx);
        PersistEvent lastEventInBatch = batch.get(lastIdx);
        batch.set(firstIdx, lastEventInBatch);
        batch.set(lastIdx, tmpEvent);
    }

    @Override
    public String toString() {

        return Objects.toStringHelper(this).add("id", id).toString();

    }

}