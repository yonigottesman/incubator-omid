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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.Futures;

import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.CommitTable.CommitTimestamp;
import org.apache.omid.metrics.Counter;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.metrics.Timer;
import org.apache.omid.transaction.Transaction.Status;
import org.apache.omid.tso.client.AbortException;
import org.apache.omid.tso.client.CellId;
import org.apache.omid.tso.client.ConnectionException;
import org.apache.omid.tso.client.ServiceUnavailableException;
import org.apache.omid.tso.client.TSOProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;


import static org.apache.omid.metrics.MetricsUtils.name;

/**
 * Omid's base abstract implementation of the {@link TransactionManager} interface.
 *
 * Provides extra methods to allow transaction manager developers to perform
 * different actions before/after the methods exposed by the {@link TransactionManager} interface.
 *
 * So, this abstract class must be extended by particular implementations of
 * transaction managers related to different storage systems (HBase...)
 */
public abstract class AbstractTransactionManager implements TransactionManager {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractTransactionManager.class);

    public final static int MAX_CHECKPOINTS_PER_TXN = 50;

    public interface TransactionFactory<T extends CellId> {

        AbstractTransaction<T> createTransaction(long transactionId, long epoch, AbstractTransactionManager tm);

    }

    private final PostCommitActions postCommitter;
    protected final TSOProtocol tsoClient;
    protected final CommitTable.Client commitTableClient;
    private final CommitTable.Writer commitTableWriter;
    private final TransactionFactory<? extends CellId> transactionFactory;

    // Metrics
    private final Timer startTimestampTimer;
    private final Timer commitTimer;
    private final Timer fenceTimer;
    private final Counter committedTxsCounter;
    private final Counter rolledbackTxsCounter;
    private final Counter errorTxsCounter;
    private final Counter invalidatedTxsCounter;

    /**
     * Base constructor
     *
     * @param metrics
     *            instrumentation metrics
     * @param postCommitter
     *            post commit action executor
     * @param tsoClient
     *            a client for accessing functionality of the status oracle
     * @param commitTableClient
     *            a client for accessing functionality of the commit table
     * @param transactionFactory
     *            a transaction factory to create the specific transaction
     *            objects required by the transaction manager being implemented.
     */
    public AbstractTransactionManager(MetricsRegistry metrics,
                                      PostCommitActions postCommitter,
                                      TSOProtocol tsoClient,
                                      CommitTable.Client commitTableClient,
                                      CommitTable.Writer commitTableWriter,
                                      TransactionFactory<? extends CellId> transactionFactory) {

        this.tsoClient = tsoClient;
        this.postCommitter = postCommitter;
        this.commitTableClient = commitTableClient;
        this.commitTableWriter = commitTableWriter;
        this.transactionFactory = transactionFactory;

        // Metrics configuration
        this.startTimestampTimer = metrics.timer(name("omid", "tm", "hbase", "startTimestamp", "latency"));
        this.commitTimer = metrics.timer(name("omid", "tm", "hbase", "commit", "latency"));
        this.fenceTimer = metrics.timer(name("omid", "tm", "hbase", "fence", "latency"));
        this.committedTxsCounter = metrics.counter(name("omid", "tm", "hbase", "committedTxs"));
        this.rolledbackTxsCounter = metrics.counter(name("omid", "tm", "hbase", "rolledbackTxs"));
        this.errorTxsCounter = metrics.counter(name("omid", "tm", "hbase", "erroredTxs"));
        this.invalidatedTxsCounter = metrics.counter(name("omid", "tm", "hbase", "invalidatedTxs"));

    }

    /**
     * Allows transaction manager developers to perform actions before creating a transaction.
     * @throws TransactionManagerException in case of any issues
     */
    public void preBegin() throws TransactionManagerException {}

    /**
     * @see org.apache.omid.transaction.TransactionManager#begin()
     */
    @Override
    public final Transaction begin() throws TransactionException {

        try {
            preBegin();

            long startTimestamp, epoch;

            // The loop is required for HA scenarios where we get the timestamp
            // but when getting the epoch, the client is connected to a new TSOServer
            // When this happen, the epoch will be larger than the startTimestamp,
            // so we need to start the transaction again. We use the fact that epoch
            // is always smaller or equal to a timestamp, and therefore, we first need
            // to get the timestamp and then the epoch.
            startTimestampTimer.start();
            try {
                do {
                    startTimestamp = tsoClient.getNewStartTimestamp().get();
                    epoch = tsoClient.getEpoch();
                } while (epoch > startTimestamp);
            } finally {
                startTimestampTimer.stop();
            }

            AbstractTransaction<? extends CellId> tx = transactionFactory.createTransaction(startTimestamp, epoch, this);

            postBegin(tx);

            return tx;
        } catch (TransactionManagerException e) {
            throw new TransactionException("An error has occured during PreBegin/PostBegin", e);
        } catch (ExecutionException e) {
            throw new TransactionException("Could not get new timestamp", e);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new TransactionException("Interrupted getting timestamp", ie);
        }
    }

    /**
     * Generates hash ID for table name, this hash is later-on sent to the TSO and used for fencing
     * @param tableName - the table name
     * @return
     */
    abstract public long getHashForTable(byte[] tableName);

    /**
     * Return the commit table client
     * @return commitTableClient
     */
    public CommitTable.Client getCommitTableClient() {
        return commitTableClient;
    }

    /**
     * @see org.apache.omid.transaction.TransactionManager#fence(byte[])
     */
    @Override
    public final Transaction fence(byte[] tableName) throws TransactionException {
        long fenceTimestamp;
        long tableID = getHashForTable(tableName); Hashing.murmur3_128().newHasher().putBytes(tableName).hash().asLong();

        try {
            fenceTimer.start();
            try {
                fenceTimestamp = tsoClient.getFence(tableID).get();
            } finally {
                fenceTimer.stop();
            }

            AbstractTransaction<? extends CellId> tx = transactionFactory.createTransaction(fenceTimestamp, fenceTimestamp, this);

            return tx;
        } catch (ExecutionException e) {
            throw new TransactionException("Could not get fence", e);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new TransactionException("Interrupted creating a fence", ie);
        }
    }

    /**
     * Allows transaction manager developers to perform actions after having started a transaction.
     * @param transaction
     *            the transaction that was just created.
     * @throws TransactionManagerException  in case of any issues
     */
    public void postBegin(AbstractTransaction<? extends CellId> transaction) throws TransactionManagerException {}

    /**
     * Allows transaction manager developers to perform actions before committing a transaction.
     * @param transaction
     *            the transaction that is going to be committed.
     * @throws TransactionManagerException  in case of any issues
     */
    public void preCommit(AbstractTransaction<? extends CellId> transaction) throws TransactionManagerException {}

    /**
     * @see org.apache.omid.transaction.TransactionManager#commit(Transaction)
     */
    @Override
    public final void commit(Transaction transaction) throws RollbackException, TransactionException {

        AbstractTransaction<? extends CellId> tx = enforceAbstractTransactionAsParam(transaction);
        enforceTransactionIsInRunningState(tx);

        if (tx.isRollbackOnly()) { // Manage explicit user rollback
            rollback(tx);
            throw new RollbackException(tx + ": Tx was set to rollback explicitly");
        }

        try {

            preCommit(tx);

            commitTimer.start();
            try {
                if (tx.getWriteSet().isEmpty() && tx.getConflictFreeWriteSet().isEmpty()) {
                    markReadOnlyTransaction(tx); // No need for read-only transactions to contact the TSO Server
                } else {
                    if (tsoClient.isLowLatency())
                        commitLowLatencyTransaction(tx);
                    else
                        commitRegularTransaction(tx);
                }
                committedTxsCounter.inc();
            } finally {
                commitTimer.stop();
            }

            postCommit(tx);

        } catch (TransactionManagerException e) {
            throw new TransactionException(e.getMessage(), e);
        }

    }

    /**
     * Allows transaction manager developers to perform actions after committing a transaction.
     * @param transaction
     *            the transaction that was committed.
     * @throws TransactionManagerException in case of any issues
     */
    public void postCommit(AbstractTransaction<? extends CellId> transaction) throws TransactionManagerException {}

    /**
     * Allows transaction manager developers to perform actions before rolling-back a transaction.
     * @param transaction the transaction that is going to be rolled-back.
     * @throws TransactionManagerException in case of any issues
     */
    public void preRollback(AbstractTransaction<? extends CellId> transaction) throws TransactionManagerException {}

    /**
     * @see org.apache.omid.transaction.TransactionManager#rollback(Transaction)
     */
    @Override
    public final void rollback(Transaction transaction) throws TransactionException {

        AbstractTransaction<? extends CellId> tx = enforceAbstractTransactionAsParam(transaction);
        enforceTransactionIsInRunningState(tx);

        try {

            preRollback(tx);

            // Make sure its commit timestamp is 0, so the cleanup does the right job
            tx.setCommitTimestamp(0);
            tx.setStatus(Status.ROLLEDBACK);

            postRollback(tx);

        } catch (TransactionManagerException e) {
            throw new TransactionException(e.getMessage(), e);
        } finally {
            tx.cleanup();
        }

    }

    /**
     * Allows transaction manager developers to perform actions after rolling-back a transaction.
     * @param transaction
     *            the transaction that was rolled-back.
     * @throws TransactionManagerException in case of any issues
     */
    public void postRollback(AbstractTransaction<? extends CellId> transaction) throws TransactionManagerException {}

    /**
     * @see java.io.Closeable#close()
     */
    @Override
    public final void close() throws IOException {

        tsoClient.close();
        commitTableClient.close();

    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helper methods
    // ----------------------------------------------------------------------------------------------------------------

    private void enforceTransactionIsInRunningState(Transaction transaction) {

        if (transaction.getStatus() != Status.RUNNING) {
            throw new IllegalArgumentException("Transaction was already " + transaction.getStatus());
        }

    }

    @SuppressWarnings("unchecked")
    // NOTE: We are sure that tx is not parametrized
    private AbstractTransaction<? extends CellId> enforceAbstractTransactionAsParam(Transaction tx) {

        if (tx instanceof AbstractTransaction) {
            return (AbstractTransaction<? extends CellId>) tx;
        } else {
            throw new IllegalArgumentException(
                    "The transaction object passed is not an instance of AbstractTransaction");
        }

    }

    private void markReadOnlyTransaction(AbstractTransaction<? extends CellId> readOnlyTx) {

        readOnlyTx.setStatus(Status.COMMITTED_RO);

    }

    private void commitLowLatencyTransaction(AbstractTransaction<? extends CellId> tx)
            throws RollbackException, TransactionException {
        try {

            long commitTs = tsoClient.commit(tx.getStartTimestamp(), tx.getWriteSet(), tx.getConflictFreeWriteSet()).get();
            boolean committed = commitTableWriter.atomicAddCommittedTransaction(tx.getStartTimestamp(),commitTs);
            if (!committed) {
                // Transaction has been invalidated by other client
                rollback(tx);
                commitTableClient.deleteCommitEntry(tx.getStartTimestamp());
                rolledbackTxsCounter.inc();
                throw new RollbackException("Transaction " + tx.getTransactionId() + " got invalidated");
            }
            certifyCommitForTx(tx, commitTs);
            updateShadowCellsAndRemoveCommitTableEntry(tx, postCommitter);

        } catch (ExecutionException e) {
            if (e.getCause() instanceof AbortException) { // TSO reports Tx conflicts as AbortExceptions in the future
                rollback(tx);
                rolledbackTxsCounter.inc();
                throw new RollbackException("Conflicts detected in tx writeset", e.getCause());
            }

            if (e.getCause() instanceof ServiceUnavailableException || e.getCause() instanceof ConnectionException) {
                errorTxsCounter.inc();
                rollback(tx); // Rollback proactively cause it's likely that a new TSOServer is now master
                throw new RollbackException(tx + " rolled-back precautionary", e.getCause());
            } else {
                throw new TransactionException(tx + ": cannot determine Tx outcome", e.getCause());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void commitRegularTransaction(AbstractTransaction<? extends CellId> tx)
            throws RollbackException, TransactionException
    {

        try {

            long commitTs = tsoClient.commit(tx.getStartTimestamp(), tx.getWriteSet(), tx.getConflictFreeWriteSet()).get();
            certifyCommitForTx(tx, commitTs);
            updateShadowCellsAndRemoveCommitTableEntry(tx, postCommitter);

        } catch (ExecutionException e) {

            if (e.getCause() instanceof AbortException) { // TSO reports Tx conflicts as AbortExceptions in the future
                rollback(tx);
                rolledbackTxsCounter.inc();
                throw new RollbackException(tx + ": Conflicts detected in writeset", e.getCause());
            }

            if (e.getCause() instanceof ServiceUnavailableException || e.getCause() instanceof ConnectionException) {

                errorTxsCounter.inc();
                try {
                    LOG.warn("Can't contact the TSO for receiving outcome for Tx {}. Checking Commit Table...", tx);
                    // Check the commit table to find if the target TSO woke up in the meantime and added the commit
                    // TODO: Decide what we should we do if we can not contact the commit table
                    Optional<CommitTimestamp> commitTimestamp =
                            commitTableClient.getCommitTimestamp(tx.getStartTimestamp()).get();
                    if (commitTimestamp.isPresent()) {
                        if (commitTimestamp.get().isValid()) {
                            LOG.warn("{}: Valid commit TS found in Commit Table. Committing Tx...", tx);
                            certifyCommitForTx(tx, commitTimestamp.get().getValue());
                            postCommitter.updateShadowCells(tx); // But do NOT remove transaction from commit table
                        } else { // Probably another Tx in a new TSO Server invalidated this transaction
                            LOG.warn("{}: Invalidated commit TS found in Commit Table. Rolling-back...", tx);
                            rollback(tx);
                            throw new RollbackException(tx + " invalidated by other Tx started", e.getCause());
                        }
                    } else {
                        LOG.warn("{}: Trying to invalidate Tx proactively in Commit Table...", tx);
                        boolean invalidated = commitTableClient.tryInvalidateTransaction(tx.getStartTimestamp()).get();
                        if (invalidated) {
                            LOG.warn("{}: Invalidated proactively in Commit Table. Rolling-back Tx...", tx);
                            invalidatedTxsCounter.inc();
                            rollback(tx); // Rollback proactively cause it's likely that a new TSOServer is now master
                            throw new RollbackException(tx + " rolled-back precautionary", e.getCause());
                        } else {
                            LOG.warn("{}: Invalidation could NOT be completed. Re-checking Commit Table...", tx);
                            // TODO: Decide what we should we do if we can not contact the commit table
                            commitTimestamp = commitTableClient.getCommitTimestamp(tx.getStartTimestamp()).get();
                            if (commitTimestamp.isPresent() && commitTimestamp.get().isValid()) {
                                LOG.warn("{}: Valid commit TS found in Commit Table. Committing Tx...", tx);
                                certifyCommitForTx(tx, commitTimestamp.get().getValue());
                                postCommitter.updateShadowCells(tx); // But do NOT remove transaction from commit table
                            } else {
                                LOG.error("{}: Can't determine Transaction outcome", tx);
                                throw new TransactionException(tx + ": cannot determine Tx outcome");
                            }
                        }
                    }
                } catch (ExecutionException e1) {
                    throw new TransactionException(tx + ": problem reading commitTS from Commit Table", e1);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                    throw new TransactionException(tx + ": interrupted while reading commitTS from Commit Table", e1);
                }
            } else {
                throw new TransactionException(tx + ": cannot determine Tx outcome", e.getCause());
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new TransactionException(tx + ": interrupted during commit", ie);

        }

    }

    private void updateShadowCellsAndRemoveCommitTableEntry(final AbstractTransaction<? extends CellId> tx,
                                                            final PostCommitActions postCommitter) {

        Futures.transform(postCommitter.updateShadowCells(tx), new Function<Void, Void>() {
            @Override
            public Void apply(Void aVoid) {
                postCommitter.removeCommitTableEntry(tx);
                return null;
            }
        });

    }

    private void certifyCommitForTx(AbstractTransaction<? extends CellId> txToSetup, long commitTS) {

        txToSetup.setStatus(Status.COMMITTED);
        txToSetup.setCommitTimestamp(commitTS);

    }

    public boolean isLowLatency() {
        return tsoClient.isLowLatency();
    }
}
