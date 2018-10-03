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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.omid.proto.TSOProto;


public class AttributeSetSnapshotFilter implements SnapshotFilter {

    private Table table;

    public AttributeSetSnapshotFilter(Table table) {
        this.table = table;
    }

    private TSOProto.Transaction.Builder getBuilder(HBaseTransaction transaction) {
        return TSOProto.Transaction.newBuilder().setTimestamp(transaction.getTransactionId())
                .setReadTimestamp(transaction.getReadTimestamp())
                .setVisibilityLevel(transaction.getVisibilityLevel().ordinal())
                .setEpoch(transaction.getEpoch());
    }

    @Override
    public Result get(Get get, HBaseTransaction transaction) throws IOException {
        get.setAttribute(CellUtils.TRANSACTION_ATTRIBUTE, getBuilder(transaction).build().toByteArray());
        get.setAttribute(CellUtils.CLIENT_GET_ATTRIBUTE, Bytes.toBytes(true));
        get.setAttribute(CellUtils.LL_ATTRIBUTE, Bytes.toBytes(transaction.isLowLatency()));

        return table.get(get);
    }

    @Override
    public ResultScanner getScanner(Scan scan, HBaseTransaction transaction) throws IOException {
        scan.setAttribute(CellUtils.TRANSACTION_ATTRIBUTE, getBuilder(transaction).build().toByteArray());
        scan.setAttribute(CellUtils.LL_ATTRIBUTE, Bytes.toBytes(transaction.isLowLatency()));
        return table.getScanner(scan);
    }
}
