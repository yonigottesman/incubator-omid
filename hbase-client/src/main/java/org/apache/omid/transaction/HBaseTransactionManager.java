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

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.hbase.HBaseCommitTable;
import org.apache.omid.committable.hbase.HBaseCommitTableConfig;
import org.apache.omid.tools.hbase.HBaseLogin;
import org.apache.omid.tso.client.CellId;
import org.apache.omid.tso.client.OmidClientConfiguration.ConflictDetectionLevel;
import org.apache.omid.tso.client.TSOClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class HBaseTransactionManager extends AbstractTransactionManager implements HBaseTransactionClient {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseTransactionManager.class);

    private static class HBaseTransactionFactory implements TransactionFactory<HBaseCellId> {

        @Override
        public HBaseTransaction createTransaction(long transactionId, long epoch, AbstractTransactionManager tm) {

            return new HBaseTransaction(transactionId, epoch, new HashSet<HBaseCellId>(), new HashSet<HBaseCellId>(),
                    tm, tm.isLowLatency());

        }

    }

    // ----------------------------------------------------------------------------------------------------------------
    // Construction
    // ----------------------------------------------------------------------------------------------------------------

    public static TransactionManager newInstance() throws IOException, InterruptedException {
        return newInstance(new HBaseOmidClientConfiguration());
    }

    public static TransactionManager newInstance(HBaseOmidClientConfiguration configuration)
            throws IOException, InterruptedException {
        //Logging in to Secure HBase if required
        HBaseLogin.loginIfNeeded(configuration);
        return builder(configuration).build();
    }

    public static class Builder {

        // Required parameters
        private final HBaseOmidClientConfiguration hbaseOmidClientConf;

        // Optional parameters - initialized to default values
        private Optional<TSOClient> tsoClient = Optional.absent();
        private Optional<CommitTable.Client> commitTableClient = Optional.absent();
        private Optional<CommitTable.Writer> commitTableWriter = Optional.absent();
        private Optional<PostCommitActions> postCommitter = Optional.absent();

        private Builder(HBaseOmidClientConfiguration hbaseOmidClientConf) {
            this.hbaseOmidClientConf = hbaseOmidClientConf;
        }

        public Builder tsoClient(TSOClient tsoClient) {
            this.tsoClient = Optional.of(tsoClient);
            return this;
        }

        public Builder commitTableClient(CommitTable.Client client) {
            this.commitTableClient = Optional.of(client);
            return this;
        }

        Builder postCommitter(PostCommitActions postCommitter) {
            this.postCommitter = Optional.of(postCommitter);
            return this;
        }

        public HBaseTransactionManager build() throws IOException, InterruptedException {

            CommitTable.Client commitTableClient = this.commitTableClient.or(buildCommitTableClient()).get();
            CommitTable.Writer commitTableWriter = this.commitTableWriter.or(buildCommitTableWriter()).get();
            PostCommitActions postCommitter = this.postCommitter.or(buildPostCommitter(commitTableClient)).get();
            TSOClient tsoClient = this.tsoClient.or(buildTSOClient()).get();

            return new HBaseTransactionManager(hbaseOmidClientConf,
                                               postCommitter,
                                               tsoClient,
                                               commitTableClient,
                                               commitTableWriter,
                                               new HBaseTransactionFactory());
        }

        private Optional<TSOClient> buildTSOClient() throws IOException, InterruptedException {
            return Optional.of(TSOClient.newInstance(hbaseOmidClientConf.getOmidClientConfiguration()));
        }


        private Optional<CommitTable.Client> buildCommitTableClient() throws IOException {
            HBaseCommitTableConfig commitTableConf = new HBaseCommitTableConfig();
            commitTableConf.setTableName(hbaseOmidClientConf.getCommitTableName());
            CommitTable commitTable = new HBaseCommitTable(hbaseOmidClientConf.getHBaseConfiguration(), commitTableConf);
            return Optional.of(commitTable.getClient());
        }

        private Optional<CommitTable.Writer> buildCommitTableWriter() throws IOException {
            HBaseCommitTableConfig commitTableConf = new HBaseCommitTableConfig();
            commitTableConf.setTableName(hbaseOmidClientConf.getCommitTableName());
            CommitTable commitTable = new HBaseCommitTable(hbaseOmidClientConf.getHBaseConfiguration(), commitTableConf);
            return Optional.of(commitTable.getWriter());
        }

        private Optional<PostCommitActions> buildPostCommitter(CommitTable.Client commitTableClient ) {

            PostCommitActions postCommitter;
            PostCommitActions syncPostCommitter = new HBaseSyncPostCommitter(hbaseOmidClientConf.getMetrics(),
                                                                             commitTableClient);
            switch(hbaseOmidClientConf.getPostCommitMode()) {
                case ASYNC:
                    ListeningExecutorService postCommitExecutor =
                            MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor(
                                    new ThreadFactoryBuilder().setNameFormat("postCommit-%d").build()));
                    postCommitter = new HBaseAsyncPostCommitter(syncPostCommitter, postCommitExecutor);
                    break;
                case SYNC:
                default:
                    postCommitter = syncPostCommitter;
                    break;
            }

            return Optional.of(postCommitter);
        }

    }

    public static Builder builder(HBaseOmidClientConfiguration hbaseOmidClientConf) {
        return new Builder(hbaseOmidClientConf);
    }

    private HBaseTransactionManager(HBaseOmidClientConfiguration hBaseOmidClientConfiguration,
                                    PostCommitActions postCommitter,
                                    TSOClient tsoClient,
                                    CommitTable.Client commitTableClient,
                                    CommitTable.Writer commitTableWriter,
                                    HBaseTransactionFactory hBaseTransactionFactory) {

        super(hBaseOmidClientConfiguration.getMetrics(),
                postCommitter,
                tsoClient,
                commitTableClient,
                commitTableWriter,
                hBaseTransactionFactory);
    }

    // ----------------------------------------------------------------------------------------------------------------
    // AbstractTransactionManager overwritten methods
    // ----------------------------------------------------------------------------------------------------------------

    @Override
    public void preCommit(AbstractTransaction<? extends CellId> transaction) throws TransactionManagerException {
        try {
            // Flush all pending writes
            HBaseTransaction hBaseTx = enforceHBaseTransactionAsParam(transaction);
            hBaseTx.flushTables();
        } catch (IOException e) {
            throw new TransactionManagerException("Exception while flushing writes", e);
        }
    }

    @Override
    public void preRollback(AbstractTransaction<? extends CellId> transaction) throws TransactionManagerException {
        try {
            // Flush all pending writes
            HBaseTransaction hBaseTx = enforceHBaseTransactionAsParam(transaction);
            hBaseTx.flushTables();
        } catch (IOException e) {
            throw new TransactionManagerException("Exception while flushing writes", e);
        }
    }

    @Override
    public long getHashForTable(byte[] tableName) {
        return HBaseCellId.getHasher().putBytes(tableName).hash().asLong();
    }

    @Override
    public long getLowWatermark() throws TransactionException {
        try {
            return commitTableClient.readLowWatermark().get();
        } catch (ExecutionException ee) {
            throw new TransactionException("Error reading low watermark", ee.getCause());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new TransactionException("Interrupted reading low watermark", ie);
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helper methods
    // ----------------------------------------------------------------------------------------------------------------

    static HBaseTransaction enforceHBaseTransactionAsParam(AbstractTransaction<? extends CellId> tx) {

        if (tx instanceof HBaseTransaction) {
            return (HBaseTransaction) tx;
        } else {
            throw new IllegalArgumentException(
                    "The transaction object passed is not an instance of HBaseTransaction");
        }

    }

    public void setConflictDetectionLevel(ConflictDetectionLevel conflictDetectionLevel) {
        tsoClient.setConflictDetectionLevel(conflictDetectionLevel);
    }

    public ConflictDetectionLevel getConflictDetectionLevel() {
        return tsoClient.getConflictDetectionLevel();
    }

    static class CommitTimestampLocatorImpl implements CommitTimestampLocator {

        private HBaseCellId hBaseCellId;
        private final Map<Long, Long> commitCache;
        private TableAccessWrapper tableAccessWrapper;

        CommitTimestampLocatorImpl(HBaseCellId hBaseCellId, Map<Long, Long> commitCache, TableAccessWrapper tableAccessWrapper) {
            this.hBaseCellId = hBaseCellId;
            this.commitCache = commitCache;
            this.tableAccessWrapper = tableAccessWrapper;
        }

        CommitTimestampLocatorImpl(HBaseCellId hBaseCellId, Map<Long, Long> commitCache) {
            this.hBaseCellId = hBaseCellId;
            this.commitCache = commitCache;
            this.tableAccessWrapper = null;
            this.tableAccessWrapper = new HTableAccessWrapper(hBaseCellId.getTable().getHTable(), hBaseCellId.getTable().getHTable());
        }

        @Override
        public Optional<Long> readCommitTimestampFromCache(long startTimestamp) {
            if (commitCache.containsKey(startTimestamp)) {
                return Optional.of(commitCache.get(startTimestamp));
            }
            return Optional.absent();
        }

        @Override
        public Optional<Long> readCommitTimestampFromShadowCell(long startTimestamp) throws IOException {

            Get get = new Get(hBaseCellId.getRow());
            byte[] family = hBaseCellId.getFamily();
            byte[] shadowCellQualifier = CellUtils.addShadowCellSuffixPrefix(hBaseCellId.getQualifier());
            get.addColumn(family, shadowCellQualifier);
            get.setMaxVersions(1);
            get.setTimeStamp(startTimestamp);
            Result result = tableAccessWrapper.get(get);
            if (result.containsColumn(family, shadowCellQualifier)) {
                return Optional.of(Bytes.toLong(result.getValue(family, shadowCellQualifier)));
            }
            return Optional.absent();
        }

    }

}
