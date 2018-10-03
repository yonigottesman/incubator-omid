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

import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.InMemoryCommitTable;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.metrics.NullMetricsProvider;
import org.apache.omid.timestamp.storage.TimestampStorage;
import org.apache.omid.tso.TSOServerConfig.TIMESTAMP_TYPE;
import org.apache.omid.tso.TimestampOracleImpl.InMemoryTimestampStorage;

import javax.inject.Named;
import javax.inject.Singleton;
import java.net.SocketException;
import java.net.UnknownHostException;

import static org.apache.omid.tso.TSOServer.TSO_HOST_AND_PORT_KEY;

public class TSOMockModule extends AbstractModule {

    private final TSOServerConfig config;

    public TSOMockModule(TSOServerConfig config) {
        Preconditions.checkArgument(config.getNumConcurrentCTWriters() >= 2, "# of Commit Table writers must be >= 2");
        this.config = config;
    }

    @Override
    protected void configure() {

        bind(TSOChannelHandler.class).in(Singleton.class);
        bind(TSOStateManager.class).to(TSOStateManagerImpl.class).in(Singleton.class);
        bind(CommitTable.class).to(InMemoryCommitTable.class).in(Singleton.class);
        bind(TimestampStorage.class).to(InMemoryTimestampStorage.class).in(Singleton.class);
        if (config.getTimestampTypeEnum() == TIMESTAMP_TYPE.WORLD_TIME) {
            bind(TimestampOracle.class).to(WorldClockOracleImpl.class).in(Singleton.class);
        } else {
            bind(TimestampOracle.class).to(PausableTimestampOracle.class).in(Singleton.class);
        }
        bind(Panicker.class).to(MockPanicker.class).in(Singleton.class);
        bind(LowWatermarkWriter.class).to(LowWatermarkWriterImpl.class).in(Singleton.class);

        install(new BatchPoolModule(config));
        install(config.getLeaseModule());
        install(new DisruptorModule(config));

    }

    @Provides
    TSOServerConfig provideTSOServerConfig() {
        return config;
    }

    @Provides
    @Singleton
    MetricsRegistry provideMetricsRegistry() {
        return new NullMetricsProvider();
    }

    @Provides
    @Named(TSO_HOST_AND_PORT_KEY)
    String provideTSOHostAndPort() throws SocketException, UnknownHostException {
        return NetworkInterfaceUtils.getTSOHostAndPort(config);
    }

    @Provides
    PersistenceProcessorHandler[] getPersistenceProcessorHandler(Provider<PersistenceProcessorHandler> provider) {
        PersistenceProcessorHandler[] persistenceProcessorHandlers = new PersistenceProcessorHandler[config.getNumConcurrentCTWriters()];
        for (int i = 0; i < persistenceProcessorHandlers.length; i++) {
            persistenceProcessorHandlers[i] = provider.get();
        }
        return persistenceProcessorHandlers;
    }

}
