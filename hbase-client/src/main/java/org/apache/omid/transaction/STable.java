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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.TransactionTimestamp;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.ImmutableList;

import org.apache.omid.tso.client.TSOClient;
import org.apache.omid.tso.client.AbortException;

/**
 * Provides transactional methods for accessing and modifying a given snapshot
 * of data identified by an opaque {@link Transaction} object. It mimics the
 * behavior in {@link org.apache.hadoop.hbase.client.HTableInterface}
 */
public class STable extends TTable {

    // ////////////////////////////////////////////////////////////////////////
    // Instantiation
    // ////////////////////////////////////////////////////////////////////////

    public STable(Configuration conf, byte[] tableName) throws IOException {
        super(conf,tableName);
    }

    public STable(Configuration conf, String tableName) throws IOException {
        super(conf, tableName);
    }

    public STable(HTableInterface hTable) throws IOException {
        super(hTable);
    }

    public STable(HTableInterface hTable, HTableInterface healerTable) throws IOException {
        super(hTable, healerTable);
    }

    // ////////////////////////////////////////////////////////////////////////
    // Transactional overridden operations
    // ////////////////////////////////////////////////////////////////////////	
    /**
     * Transactional version of {@link HTableInterface#put(Put put)} for Amos
     */
    @Override
    public void put(Transaction tx, Put put) throws IOException {
    	
    	throwExceptionIfOpSetsTimerange(put);

        HBaseTransaction transaction = enforceHBaseTransactionAsParam(tx);

        final long startTimestamp = transaction.getStartTimestamp();
        // create put with correct ts
        final Put tsput = new Put(put.getRow(), startTimestamp);

        HBaseCellId leader = transaction.getLeader();
        if (leader == null)
        {
            //Just get first cell in put and use as leader
            Cell leaderCell = put.getFamilyCellMap().firstEntry().getValue().get(0);

            HBaseCellId leaderCellID = new HBaseCellId(table,
                    CellUtil.cloneRow(leaderCell),
                    CellUtil.cloneFamily(leaderCell),
                    CellUtil.cloneQualifier(leaderCell),
                    startTimestamp);

            transaction.setLeader(leaderCellID);
            leader = leaderCellID;
        }

        Map<byte[], List<Cell>> kvs = put.getFamilyCellMap();
        for (List<Cell> kvl : kvs.values()) {
            for (Cell c : kvl) {
                CellUtils.validateCell(c, startTimestamp);
                // Reach into keyvalue to update timestamp.
                // It's not nice to reach into keyvalue internals,
                // but we want to avoid having to copy the whole thing
                KeyValue kv = KeyValueUtil.ensureKeyValue(c);
                Bytes.putLong(kv.getValueArray(), kv.getTimestampOffset(), startTimestamp);
                tsput.add(kv);

                HBaseCellId cellId = new HBaseCellId(table,
                        CellUtil.cloneRow(kv),
                        CellUtil.cloneFamily(kv),
                        CellUtil.cloneQualifier(kv),
                        kv.getTimestamp());

                transaction.addWriteSetElement(cellId);

                //add the leader cell
                tsput.add(cellId.getFamily(),
                        CellUtils.addLeaderCellSuffix(cellId.getQualifier()),
                        startTimestamp,
                        Bytes.toBytes(leader.toString()));
            }
        }
        
        // Check only the first cell
        // TODO: Should check all cells in the put operation (in the HRegion - not here!)
        Cell cell = kvs.values().iterator().next().get(0);
        byte[] family = CellUtil.cloneFamily(cell);
        byte[] qualifier = CellUtils.addShadowCellSuffix(CellUtil.cloneQualifier(cell));

        if (!table.checkTxnAndPut(put.getRow(), family, qualifier, Bytes.toBytes(startTimestamp), tsput)) {
        	System.out.println("Txn put aborted - conflicting commit");
        	transaction.setRollbackOnly();
        }
        
    }

    
    // ////////////////////////////////////////////////////////////////////////
    // Singleton Transaction operations
    // ////////////////////////////////////////////////////////////////////////

    /**
     * Singleton Transactional version of {@link HTableInterface#get(Get get)} for singletons
     * 
     * According to the current implementation, the version returned when called with
     * a single column is the latest commit when the get was called (not when the value returns)
     */
    public Result singletonGet(final Get get) throws IOException {

        throwExceptionIfOpSetsTimerange(get);
        throwExceptionIfMultiGet(get);

        final Get tsget = new Get(get.getRow()).setFilter(get.getFilter());
        TimeRange timeRange = get.getTimeRange();
        long startTime = timeRange.getMin(); // should return 0L
        long endTime = Math.min(timeRange.getMax(), TransactionTimestamp.SINGLETON_TIMESTAMP);
        tsget.setTimeRange(startTime, endTime).setMaxVersions(1);
        Map<byte[], NavigableSet<byte[]>> kvs = get.getFamilyMap();
        for (Map.Entry<byte[], NavigableSet<byte[]>> entry : kvs.entrySet()) {
            byte[] family = entry.getKey();
            NavigableSet<byte[]> qualifiers = entry.getValue();
            if (qualifiers == null || qualifiers.isEmpty()) {
                tsget.addFamily(family);
            } else {
                for (byte[] qualifier : qualifiers) {
                    tsget.addColumn(family, qualifier);
                    tsget.addColumn(family, CellUtils.addShadowCellSuffix(qualifier));
                }
            }
        }
        LOG.trace("Initial Get = {}", tsget);
        tsget.addFamily(Bytes.toBytes("GET_LOCAL_COUNTER"));
        // Return the KVs that belong to the transaction snapshot, ask for more
        // versions if needed
        Result result = table.get(tsget);
        List<Cell> filteredKeyValues = new ArrayList<Cell>();
        Long latestTS = new Long(Long.MAX_VALUE);

        /*
        * Must fix this!
        * If Hbase removes early versions of a cell that doesnt have a SC but has a __TS__ SC
        * */
//        if (!result.isEmpty() && !filterCellsForSingletonSnapshot(result.listCells(), filteredKeyValues, latestTS)) {
//            //System.out.println("Error read");
//        }
//
//        return Result.create(filteredKeyValues);




        while (!result.isEmpty() && !filterCellsForSingletonSnapshot(result.listCells(), filteredKeyValues, latestTS)) {
            if (tsget.getMaxVersions() > 256) // limit the number of gets
        		tsget.setMaxVersions(); // just get all versions
        	else
        		tsget.setMaxVersions(tsget.getMaxVersions() * 2); // double number of versions
        	tsget.setTimeRange(startTime, latestTS);
        	result = table.get(tsget);
            System.out.format("ERROR %s \n",Bytes.toString(tsget.getRow()));
        }

        for (Cell cell :result.listCells()) {
            //find the local clock
            if (Bytes.compareTo(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),Bytes.toBytes("GET_LOCAL_COUNTER"), 0,17) == 0) {
                filteredKeyValues.add(cell);
            }
        }

        return Result.create(filteredKeyValues);

    } 

    /**
     * Check the cells to see if they contain a valid snapshot. A valid snapshot is a 
     * snapshot in which all cells contain committed values which are either the most updated versions of the cells
     * or are all smaller than the smallest tentative value of all cells.
     * @param rawCells a list of the cells to be checked and filtered
     * @param filteredKeyValues the result of the filtering 
     * @param latestTS used to update the latest timestamp for the next iteration - this is the timestamp of the first tentative cell encountered
     * @return true if filteredKeyValues contains a vaid snapshot and false if not - in that case, latestTS holds the latest timestamp with 
     * which a new get should be issued.
     */
    private boolean filterCellsForSingletonSnapshot(List<Cell> rawCells,
			List<Cell> filteredKeyValues, Long latestTS) {

    	assert (rawCells != null && filteredKeyValues != null);
        List<Cell> keyValuesInSnapshot = new ArrayList<>();
        //List<Get> pendingGetsList = new ArrayList<>();
        
        Map<Long, Long> commitCache = buildCommitCache(rawCells);
        
        ImmutableList<Collection<Cell>> cellsByColumnFilteringShadowCells = groupCellsByColumnFilteringShadowCells(rawCells);
        
        for (Collection<Cell> columnCells : cellsByColumnFilteringShadowCells) {
            boolean snapshotValueFound = false;
            for (Cell cell : columnCells) {
                //System.out.format("YONI cell %s YONI\n",Bytes.toString(cell.getQualifierArray()));
                if (isCellTentative(cell,commitCache)) {
            		latestTS = cell.getTimestamp();
            		return false; // should issue another get with the updated timeRange
            	} else if (commitCache.get(cell.getTimestamp()) < latestTS) {
                    if (!CellUtil.matchingValue(cell, CellUtils.DELETE_TOMBSTONE)) {
                        keyValuesInSnapshot.add(cell);
                    }
                    snapshotValueFound = true;
                    break;
            	}
            }
            if (!snapshotValueFound) {
                return false;
            }
        }

        Collections.sort(keyValuesInSnapshot, KeyValue.COMPARATOR);
 
        assert (keyValuesInSnapshot.size() <= rawCells.size());
//        System.out.println("keyValuesInSnapshot size = " + keyValuesInSnapshot.size());
        filteredKeyValues.addAll(keyValuesInSnapshot);

		return true;
	}

	private boolean isCellTentative(Cell cell, Map<Long, Long> commitCache) {
		if (commitCache.containsKey(cell.getTimestamp()))
			return false;
		return true;
	}

	private void throwExceptionIfMultiGet(Get get) {
    	throwExceptionIfMultiFamily(get);
    	@SuppressWarnings("unchecked")
		NavigableSet<byte []> cols = (NavigableSet<byte[]>) get.getFamilyMap().values().toArray()[0];
    	if (cols.size() != 1) {
            throw new IllegalArgumentException(
                    "Only a single column is currently allowed when using Singleton Operations");
        }
	}

	private void throwExceptionIfMultiFamily(Get get) {
		if (get.numFamilies() != 1) {
            throw new IllegalArgumentException(
                    "Only a single column family is allowed when using Singleton Operations");
		}
	}

	/**
     * Transactional version of {@link HTableInterface#delete(Delete delete)} for singletons
     */
    public void singletonDelete(Delete delete) throws IOException {

        throwExceptionIfOpSetsTimerange(delete);

        //HBaseTransaction transaction = enforceHBaseTransactionAsParam(tx);

        final long startTimestamp = 1; //transaction.getStartTimestamp();
        boolean issueGet = false;

        final Put deleteP = new Put(delete.getRow(), startTimestamp);
        final Get deleteG = new Get(delete.getRow());
        Map<byte[], List<Cell>> fmap = delete.getFamilyCellMap();
        if (fmap.isEmpty()) {
            issueGet = true;
        }
        for (List<Cell> cells : fmap.values()) {
            for (Cell cell : cells) {
                CellUtils.validateCell(cell, startTimestamp);
                switch (KeyValue.Type.codeToType(cell.getTypeByte())) {
                case DeleteColumn:
                    deleteP.add(CellUtil.cloneFamily(cell),
                                CellUtil.cloneQualifier(cell),
                                startTimestamp,
                                CellUtils.DELETE_TOMBSTONE);
                    break;
                case DeleteFamily:
                    deleteG.addFamily(CellUtil.cloneFamily(cell));
                    issueGet = true;
                    break;
                case Delete:
                    if (cell.getTimestamp() == HConstants.LATEST_TIMESTAMP) {
                        deleteP.add(CellUtil.cloneFamily(cell),
                                    CellUtil.cloneQualifier(cell),
                                    startTimestamp,
                                    CellUtils.DELETE_TOMBSTONE);
                        //transaction.addWriteSetElement(
//                                                       new HBaseCellId(table,
//                                                                       delete.getRow(),
//                                                                       CellUtil.cloneFamily(cell),
//                                                                       CellUtil.cloneQualifier(cell),
//                                                                       cell.getTimestamp()));
                        break;
                    } else {
                        throw new UnsupportedOperationException(
                                "Cannot delete specific versions on Snapshot Isolation.");
                    }
                default:
                    break;
                }
            }
        }
        if (issueGet) {
            // It's better to perform a transactional get to avoid deleting more
            // than necessary
            Result result = new Result(); //.get(transaction, deleteG);
            if (!result.isEmpty()) {
                for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entryF : result.getMap()
                        .entrySet()) {
                    byte[] family = entryF.getKey();
                    for (Entry<byte[], NavigableMap<Long, byte[]>> entryQ : entryF.getValue().entrySet()) {
                        byte[] qualifier = entryQ.getKey();
                        deleteP.add(family, qualifier, CellUtils.DELETE_TOMBSTONE);
                        //transaction.addWriteSetElement(new HBaseCellId(table, delete.getRow(), family, qualifier, transaction.getStartTimestamp()));
                    }
                }
            }
        }

        if (!deleteP.isEmpty()) {
            table.put(deleteP);
        }

    }

    /**
     * BWC
     * Transactional version of {@link HTableInterface#put(Put put)} for Singeltons
     * @throws AbortException if the put has to be aborted due to ongoing transactions
     */
    public void singletonPut(Put put) throws IOException, AbortException {

        throwExceptionIfOpSetsTimerange(put);
        
        throwExceptionIfMultiPut(put);

        // create put with correct ts
        final Put tsput = new Put(put.getRow(), TransactionTimestamp.SINGLETON_TIMESTAMP);
        Map<byte[], List<Cell>> kvs = put.getFamilyCellMap();
        for (List<Cell> kvl : kvs.values()) {
            for (Cell c : kvl) {
                CellUtils.validateCell(c, TransactionTimestamp.SINGLETON_TIMESTAMP);
                // Reach into keyvalue to update timestamp.
                // It's not nice to reach into keyvalue internals,
                // but we want to avoid having to copy the whole thing
                KeyValue kv = KeyValueUtil.ensureKeyValue(c);
                //Bytes.putLong(kv.getValueArray(), kv.getTimestampOffset(), 1234);
                tsput.add(kv);
                // Add the shadow cell 
                tsput.add(CellUtil.cloneFamily(c), 
                		CellUtils.addShadowCellSuffix(CellUtil.cloneQualifier(c)),
                		HConstants.LATEST_TIMESTAMP,					// This is later updated to the local HRegion timestamp
                		Bytes.toBytes(TransactionTimestamp.SINGLETON_TIMESTAMP));
            }
        }

        // Get the first cell (just in order not to break the API)
        Cell cell = kvs.values().iterator().next().get(0);
        byte[] family = CellUtil.cloneFamily(cell);
        byte[] qualifier = CellUtil.cloneQualifier(cell);
        
        if (!table.checkSingletonAndPut(put.getRow(), family, qualifier, null, tsput))
        	throw new AbortException();
    }

    /**
     * WC
     * Transactional version of {@link HTableInterface#put(Put put)} for Singeltons
     * @throws AbortException if the put has to be aborted due to ongoing transactions
     */
    public void singletonPutCommit(Put put,long bts) throws IOException, AbortException {

        throwExceptionIfOpSetsTimerange(put);

        throwExceptionIfMultiPut(put);

        // create put with correct ts
        final Put tsput = new Put(put.getRow(), TransactionTimestamp.SINGLETON_TIMESTAMP);
        Map<byte[], List<Cell>> kvs = put.getFamilyCellMap();
        for (List<Cell> kvl : kvs.values()) {
            for (Cell c : kvl) {
                CellUtils.validateCell(c, TransactionTimestamp.SINGLETON_TIMESTAMP);
                // Reach into keyvalue to update timestamp.
                // It's not nice to reach into keyvalue internals,
                // but we want to avoid having to copy the whole thing
                KeyValue kv = KeyValueUtil.ensureKeyValue(c);
                //pass the begin ts, this value is checked in hbase and then written over with local clock
                Bytes.putLong(kv.getValueArray(), kv.getTimestampOffset(), bts);
                tsput.add(kv);
                // Add the shadow cell
                tsput.add(CellUtil.cloneFamily(c),
                        CellUtils.addShadowCellSuffix(CellUtil.cloneQualifier(c)),
                        HConstants.LATEST_TIMESTAMP,					// This is later updated to the local HRegion timestamp
                        Bytes.toBytes(TransactionTimestamp.SINGLETON_TIMESTAMP));
            }
        }

        // Get the first cell (just in order not to break the API)
        Cell cell = kvs.values().iterator().next().get(0);
        byte[] family = CellUtil.cloneFamily(cell);
        byte[] qualifier = CellUtil.cloneQualifier(cell);

        if (!table.checkSingletonAndPut(put.getRow(), family, qualifier, null, tsput))
            throw new AbortException();
    }




    private boolean isCellSingleton(Cell cell) {
		return TransactionTimestamp.isSingleton(cell.getTimestamp());
	}

	private void throwExceptionIfMultiPut(Put put) {
		if (put.numFamilies() != 1) {
            throw new IllegalArgumentException(
                    "Only a single column family is allowed when using Singleton Operations");
		}
		@SuppressWarnings("unchecked")
		List<Cell> cells = (List<Cell>) put.getFamilyCellMap().values().toArray()[0];
		if (cells.size() != 1) {
			throw new IllegalArgumentException(
                    "Only a single column is allowed when using Singleton Operations");
		}
	}

	@Override
	protected Map<Long, Long> buildCommitCache(List<Cell> rawCells) {

		Map<Long, Long> commitCache = new HashMap<>();

        for (Cell cell : rawCells) {
            if (CellUtils.isShadowCell(cell)) {
            	long ts = cell.getTimestamp();
            	long val = Bytes.toLong(CellUtil.cloneValue(cell));
            	if (val == TransactionTimestamp.SINGLETON_TIMESTAMP) // this makes sure singleton commits are handled correctly
            			val = ts;
                commitCache.put(ts, val);
            }
        }

        return commitCache;
    }
}
