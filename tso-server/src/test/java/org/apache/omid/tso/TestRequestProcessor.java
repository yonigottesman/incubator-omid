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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.SettableFuture;

import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.metrics.NullMetricsProvider;
import org.apache.omid.transaction.AbstractTransactionManager;
import org.jboss.netty.channel.Channel;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertTrue;

public class TestRequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(TestRequestProcessor.class);

    private static final int CONFLICT_MAP_SIZE = 1000;
    private static final int CONFLICT_MAP_ASSOCIATIVITY = 32;

    private MetricsRegistry metrics = new NullMetricsProvider();

    private PersistenceProcessor persist;

    private TSOStateManager stateManager;

    // Request processor under test
    private RequestProcessor requestProc;

    private LowWatermarkWriter lowWatermarkWriter;

    @BeforeMethod
    public void beforeMethod() throws Exception {

        // Build the required scaffolding for the test
        MetricsRegistry metrics = new NullMetricsProvider();

        TimestampOracleImpl timestampOracle =
                new TimestampOracleImpl(metrics, new TimestampOracleImpl.InMemoryTimestampStorage(), new MockPanicker());

        stateManager = new TSOStateManagerImpl(timestampOracle);
        lowWatermarkWriter = mock(LowWatermarkWriter.class);
        persist = mock(PersistenceProcessor.class);
        SettableFuture<Void> f = SettableFuture.create();
        f.set(null);
        doReturn(f).when(lowWatermarkWriter).persistLowWatermark(any(Long.class));

        TSOServerConfig config = new TSOServerConfig();
        config.setConflictMapSize(CONFLICT_MAP_SIZE);

        requestProc = new RequestProcessorPersistCT(metrics, timestampOracle, persist, new MockPanicker(), config, lowWatermarkWriter);

        // Initialize the state for the experiment
        stateManager.register(requestProc);
        stateManager.initialize();

    }

    @Test(timeOut = 30_000)
    public void testTimestamp() throws Exception {

        requestProc.timestampRequest(null, new MonitoringContextImpl(metrics));
        ArgumentCaptor<Long> firstTScapture = ArgumentCaptor.forClass(Long.class);
        verify(persist, timeout(100).times(1)).addTimestampToBatch(
                firstTScapture.capture(), any(Channel.class), any(MonitoringContextImpl.class));

        long firstTS = firstTScapture.getValue();
        // verify that timestamps increase monotonically
        for (int i = 0; i < 100; i++) {
            requestProc.timestampRequest(null, new MonitoringContextImpl(metrics));
            verify(persist, timeout(100).times(1)).addTimestampToBatch(eq(firstTS), any(Channel.class), any(MonitoringContext.class));
            firstTS += AbstractTransactionManager.MAX_CHECKPOINTS_PER_TXN;
        }

    }

    @Test(timeOut = 30_000)
    public void testCommit() throws Exception {

        requestProc.timestampRequest(null, new MonitoringContextImpl(metrics));
        ArgumentCaptor<Long> TScapture = ArgumentCaptor.forClass(Long.class);
        verify(persist, timeout(100).times(1)).addTimestampToBatch(
                TScapture.capture(), any(Channel.class), any(MonitoringContextImpl.class));
        long firstTS = TScapture.getValue();

        List<Long> writeSet = Lists.newArrayList(1L, 20L, 203L);
        requestProc.commitRequest(firstTS - AbstractTransactionManager.MAX_CHECKPOINTS_PER_TXN, writeSet, new ArrayList<Long>(0), false, null, new MonitoringContextImpl(metrics));
        verify(persist, timeout(100).times(1)).addAbortToBatch(eq(firstTS - AbstractTransactionManager.MAX_CHECKPOINTS_PER_TXN), any(Channel.class), any(MonitoringContext.class));

        requestProc.commitRequest(firstTS, writeSet, new ArrayList<Long>(0), false, null, new MonitoringContextImpl(metrics));
        ArgumentCaptor<Long> commitTScapture = ArgumentCaptor.forClass(Long.class);

        verify(persist, timeout(100).times(1)).addCommitToBatch(eq(firstTS), commitTScapture.capture(), any(Channel.class), any(MonitoringContext.class));
        assertTrue(commitTScapture.getValue() > firstTS, "Commit TS must be greater than start TS");

        // test conflict
        requestProc.timestampRequest(null, new MonitoringContextImpl(metrics));
        TScapture = ArgumentCaptor.forClass(Long.class);
        verify(persist, timeout(100).times(2)).addTimestampToBatch(
                TScapture.capture(), any(Channel.class), any(MonitoringContextImpl.class));
        long secondTS = TScapture.getValue();

        requestProc.timestampRequest(null, new MonitoringContextImpl(metrics));
        TScapture = ArgumentCaptor.forClass(Long.class);
        verify(persist, timeout(100).times(3)).addTimestampToBatch(
                TScapture.capture(), any(Channel.class), any(MonitoringContextImpl.class));
        long thirdTS = TScapture.getValue();

        requestProc.commitRequest(thirdTS, writeSet, new ArrayList<Long>(0), false, null, new MonitoringContextImpl(metrics));
        verify(persist, timeout(100).times(1)).addCommitToBatch(eq(thirdTS), anyLong(), any(Channel.class), any(MonitoringContextImpl.class));
        requestProc.commitRequest(secondTS, writeSet, new ArrayList<Long>(0), false, null, new MonitoringContextImpl(metrics));
        verify(persist, timeout(100).times(1)).addAbortToBatch(eq(secondTS), any(Channel.class), any(MonitoringContextImpl.class));

    }

    @Test(timeOut = 30_000)
    public void testFence() throws Exception {

        requestProc.fenceRequest(666L, null, new MonitoringContextImpl(metrics));
        ArgumentCaptor<Long> firstTScapture = ArgumentCaptor.forClass(Long.class);
        verify(persist, timeout(100).times(1)).addFenceToBatch(eq(666L),
                firstTScapture.capture(), any(Channel.class), any(MonitoringContext.class));

    }

    @Test(timeOut = 30_000)
    public void testCommitRequestAbortsWhenResettingRequestProcessorState() throws Exception {

        List<Long> writeSet = Collections.emptyList();

        // Start a transaction...
        requestProc.timestampRequest(null, new MonitoringContextImpl(metrics));
        ArgumentCaptor<Long> capturedTS = ArgumentCaptor.forClass(Long.class);
        verify(persist, timeout(100).times(1)).addTimestampToBatch(capturedTS.capture(),
                                                                   any(Channel.class),
                                                                   any(MonitoringContextImpl.class));
        long startTS = capturedTS.getValue();

        // ... simulate the reset of the RequestProcessor state (e.g. due to
        // a change in mastership) and...
        stateManager.initialize();

        // ...check that the transaction is aborted when trying to commit
        requestProc.commitRequest(startTS, writeSet, new ArrayList<Long>(0), false, null, new MonitoringContextImpl(metrics));
        verify(persist, timeout(100).times(1)).addAbortToBatch(eq(startTS), any(Channel.class), any(MonitoringContextImpl.class));

    }

    @Test(timeOut = 5_000)
    public void testLowWatermarkIsStoredOnlyWhenACacheElementIsEvicted() throws Exception {

        final int ANY_START_TS = 1;
        final long FIRST_COMMIT_TS_EVICTED = AbstractTransactionManager.MAX_CHECKPOINTS_PER_TXN;
        final long NEXT_COMMIT_TS_THAT_SHOULD_BE_EVICTED = FIRST_COMMIT_TS_EVICTED + AbstractTransactionManager.MAX_CHECKPOINTS_PER_TXN;

        // Fill the cache to provoke a cache eviction
        for (long i = 0; i < CONFLICT_MAP_SIZE + CONFLICT_MAP_ASSOCIATIVITY; i++) {
            long writeSetElementHash = i + 1; // This is to match the assigned CT: K/V in cache = WS Element Hash/CT
            List<Long> writeSet = Lists.newArrayList(writeSetElementHash);
            requestProc.commitRequest(ANY_START_TS, writeSet, new ArrayList<Long>(0), false, null, new MonitoringContextImpl(metrics));
        }

        Thread.currentThread().sleep(3000); // Allow the Request processor to finish the request processing

        // Check that first time its called is on init
        verify(lowWatermarkWriter, timeout(100).times(1)).persistLowWatermark(eq(0L));
        // Then, check it is called when cache is full and the first element is evicted (should be a AbstractTransactionManager.NUM_OF_CHECKPOINTS)
        verify(lowWatermarkWriter, timeout(100).times(1)).persistLowWatermark(eq(FIRST_COMMIT_TS_EVICTED));
        // Finally it should never be called with the next element
        verify(lowWatermarkWriter, timeout(100).never()).persistLowWatermark(eq(NEXT_COMMIT_TS_THAT_SHOULD_BE_EVICTED));

    }

}