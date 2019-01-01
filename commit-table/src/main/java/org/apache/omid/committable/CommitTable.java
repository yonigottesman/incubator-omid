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
package org.apache.omid.committable;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.io.IOException;

public interface CommitTable {

    long INVALID_TRANSACTION_MARKER = -1L;

    Writer getWriter() throws IOException;

    Client getClient() throws IOException;

    interface Writer extends Closeable {

        void addCommittedTransaction(long startTimestamp, long commitTimestamp) throws IOException;

        void updateLowWatermark(long lowWatermark) throws IOException;

        /**
         * Flushes all the buffered events to the underlying datastore
         */
        void flush() throws IOException;

        /**
         * Allows to clean the write's current buffer. It is required for HA
         */
        void clearWriteBuffer();

        /**
         * Add commited transaction while checking if invalidated by other client
         */
        boolean atomicAddCommittedTransaction(long startTimestamp, long commitTimestamp) throws IOException;
    }

    interface Client extends Closeable {

        /**
         * Checks whether a transaction commit data is inside the commit table The function also checks whether the
         * transaction was invalidated and returns a commit timestamp type accordingly.
         *
         * @param startTimestamp the transaction start timestamp
         * @return Optional of CommitTimestamp that represents a valid, invalid, or no timestamp.
         */
        ListenableFuture<Optional<CommitTimestamp>> getCommitTimestamp(long startTimestamp);

        ListenableFuture<Long> readLowWatermark();

        ListenableFuture<Void> deleteCommitEntry(long startTimestamp);

        /**
         * Atomically tries to invalidate a non-committed transaction launched by a previous TSO server.
         *
         * @param startTimeStamp the transaction to invalidate
         * @return true on success and false on failure
         */
        ListenableFuture<Boolean> tryInvalidateTransaction(long startTimeStamp);
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helper classes
    // ----------------------------------------------------------------------------------------------------------------
    class CommitTimestamp {

        public enum Location {
            NOT_PRESENT, CACHE, COMMIT_TABLE, SHADOW_CELL
        }

        private final Location location;
        private final long value;
        private final boolean isValid;

        public CommitTimestamp(Location location, long value, boolean isValid) {
            this.location = location;
            this.value = value;
            this.isValid = isValid;
        }

        public Location getLocation() {
            return location;
        }

        public long getValue() {
            return value;
        }

        public boolean isValid() {
            return isValid;
        }

        @Override
        public String toString() {
            return String.format("Is valid=%s, Location=%s, Value=%d)", isValid, location, value);
        }

    }

}
