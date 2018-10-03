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
package org.apache.omid.transaction;

import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.hbase.HBaseCommitTable;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.metrics.NullMetricsProvider;
import org.apache.omid.timestamp.storage.HBaseTimestampStorage;
import org.apache.omid.timestamp.storage.TimestampStorage;
import org.apache.omid.tso.BatchPoolModule;
import org.apache.omid.tso.DisruptorModule;
import org.apache.omid.tso.LeaseManagement;
import org.apache.omid.tso.LowWatermarkWriter;
import org.apache.omid.tso.LowWatermarkWriterImpl;
import org.apache.omid.tso.MockPanicker;
import org.apache.omid.tso.NetworkInterfaceUtils;
import org.apache.omid.tso.Panicker;
import org.apache.omid.tso.PersistenceProcessorHandler;
import org.apache.omid.tso.TSOChannelHandler;
import org.apache.omid.tso.TSOServerConfig;
import org.apache.omid.tso.TSOStateManager;
import org.apache.omid.tso.TSOStateManagerImpl;
import org.apache.omid.tso.TimestampOracle;
import org.apache.omid.tso.TimestampOracleImpl;
import org.apache.omid.tso.VoidLeaseManager;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import javax.inject.Named;
import javax.inject.Singleton;

import java.io.File;
import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;

import static org.apache.omid.tso.TSOServer.TSO_HOST_AND_PORT_KEY;

class TSOForHBaseCompactorTestModule extends AbstractModule {

    private final TSOServerConfig config;

    TSOForHBaseCompactorTestModule(TSOServerConfig config) {
        this.config = config;
    }

    @Override
    protected void configure() {

        bind(TSOChannelHandler.class).in(Singleton.class);

        bind(TSOStateManager.class).to(TSOStateManagerImpl.class).in(Singleton.class);

        bind(Panicker.class).to(MockPanicker.class);
        // HBase commit table creation
        bind(CommitTable.class).to(HBaseCommitTable.class).in(Singleton.class);
        // Timestamp storage creation
        bind(TimestampStorage.class).to(HBaseTimestampStorage.class).in(Singleton.class);
        bind(TimestampOracle.class).to(TimestampOracleImpl.class).in(Singleton.class);
        bind(LowWatermarkWriter.class).to(LowWatermarkWriterImpl.class).in(Singleton.class);
        install(new BatchPoolModule(config));
        // DisruptorConfig
        install(new DisruptorModule(config));

    }

    @Provides
    @Singleton
    Configuration provideHBaseConfig() throws IOException {
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.setBoolean("hbase.localcluster.assign.random.ports",true);
        hbaseConf.setInt("hbase.hregion.memstore.flush.size", 10_000 * 1024);
        hbaseConf.setInt("hbase.regionserver.nbreservationblocks", 1);
        hbaseConf.set("tso.host", "localhost");
        hbaseConf.setInt("tso.port", 1234);
        hbaseConf.set("hbase.coprocessor.region.classes", "org.apache.omid.transaction.OmidCompactor");
        final String rootdir = "/tmp/hbase.test.dir/";
        File rootdirFile = new File(rootdir);
        FileUtils.deleteDirectory(rootdirFile);
        hbaseConf.set("hbase.rootdir", rootdir);
        return hbaseConf;
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
    @Singleton
    LeaseManagement provideLeaseManager(TSOChannelHandler tsoChannelHandler,
                                        TSOStateManager stateManager) throws IOException {
        return new VoidLeaseManager(tsoChannelHandler, stateManager);
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