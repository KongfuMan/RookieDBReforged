package org.csfundamental.database;

import org.csfundamental.database.buffer.BufferManager;
import org.csfundamental.database.common.PredicateOperator;
import org.csfundamental.database.common.iterator.BacktrackingIterator;
import org.csfundamental.database.query.QueryPlan;
import org.csfundamental.database.recovery.RecoveryManager;
import org.csfundamental.database.table.Record;
import org.csfundamental.database.table.RecordId;
import org.csfundamental.database.table.Schema;
import org.csfundamental.database.table.Table;
import org.csfundamental.database.table.databox.DataBox;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Database objects keeps track of transactions, tables, and indices
 * and delegates work to its buffer manager, disk manager, lock manager and
 * recovery manager.
 *
 * A Database instance operates on files in the directory specified by the `fileDir`
 * argument in the constructors. The files in this directory will be modified by
 * a DiskSpaceManager instance. Upon starting up for the following partitions
 * are allocated through the disk space manager:
 *  - Partition 0: used for log records from the recovery manager
 *  - Partition 1: used by the _metadata.tables table, which persists
 *    information about user created tables
 *  - Partition 2: used by the _metadata.indices table, which persists
 *    information about user created indices
 *
 * Each partition corresponds to a file in `fileDir`. The remaining partitions
 * are used for user created tables and are allocated as tables are created.
 *
 * Metadata tables are manually synchronized and use a special locking hierarchy
 * to improve concurrency. The methods to lock and access metadata has already
 * been implemented.
 * - _metadata.tables is a child resource of the database. The children of
 *   _metadata.tables are the names of a regular user tables. For example, if a
 *   user wants exclusive access on the metadata of the table `myTable`, they
 *   would have to acquire an X lock on the resource `database/_metadata.tables/mytable`.
 *
 * - _metadata.indices is a child resource of the database. The children of
 *   _metadata.indices are the names of regular user tables. The grandchildren are
 *   the names of indices. For example, if a user wants to get shared access to
 *   the index on column `rowId` of `someTable`, they would need to acquire an
 *   S lock on `database/_metadata.indices/someTable/rowId`. If a user wanted
 *   to acquire exclusive access on all of the indices of `someTable` (for example
 *   to insert a new record into every index) they would need to acquire an
 *   X lock on `database/_metadata.indices/someTable`.
 */
public class Database implements AutoCloseable {
    private String fileDir;
    private int numMemoryPages;
    private RecoveryManager recoveryManager;
    private BufferManager bufferManager;
    private Table metadata;
    private Table logTable;
    private Map<String, Table> tables;

    public Database(String fileDir, int numMemoryPages) {
        this.fileDir = fileDir;
        this.numMemoryPages = numMemoryPages;
    }

    public void setWorkMem(int workMem) {
    }

    public void loadDemo() {
    }

    public Transaction beginTransaction() {
        return null;
    }

    @Override
    public void close() {
    }

    public class TransactionImpl extends Transaction {
        private long transNum;
        private boolean isRecovery;
        private TransactionContext transContext;

        @Override
        public Optional<QueryPlan> execute(String statement) {
            return Optional.empty();
        }

        @Override
        public long getTransNum() {
            return this.transNum;
        }

        @Override
        public void cleanup() {

        }

        @Override
        public void createTable(Schema s, String tableName) {
            // insert schema to metadata table

        }

        @Override
        public void dropTable(String tableName) {

        }

        @Override
        public void dropAllTables() {

        }

        @Override
        public void createIndex(String tableName, String columnName, boolean bulkLoad) {

        }

        @Override
        public void dropIndex(String tableName, String columnName) {

        }

        @Override
        public QueryPlan query(String tableName) {
            return null;
        }

        @Override
        public QueryPlan query(String tableName, String alias) {
            return null;
        }

        @Override
        public void insert(String tableName, Record record) {
            // insert into table
            // 1. get schema
            // 2. verify record
            // 3. WAL
            // 4. do actual insertion
            // 5. add to index tree.
            Schema schema = this.getSchema(tableName);
            schema.verify(record);
            // TODO: write WAL using RecoveryManager

            Table table = tables.get(tableName);
            table.addRecord(record);

            // TODO: add to index tree if necessary.

        }

        @Override
        public void update(String tableName, String targetColumnName, UnaryOperator<DataBox> targetValue) {

        }

        @Override
        public void update(String tableName, String targetColumnName, UnaryOperator<DataBox> targetValue, String predColumnName, PredicateOperator predOperator, DataBox predValue) {

        }

        @Override
        public void update(String tableName, String targetColumnName, Function<Record, DataBox> expr, Function<Record, DataBox> cond) {

        }

        @Override
        public void delete(String tableName, String predColumnName, PredicateOperator predOperator, DataBox predValue) {

        }

        @Override
        public void delete(String tableName, Function<Record, DataBox> cond) {

        }

        @Override
        public void savepoint(String savepointName) {

        }

        @Override
        public void rollbackToSavepoint(String savepointName) {

        }

        @Override
        public void releaseSavepoint(String savepointName) {

        }

        @Override
        public Schema getSchema(String tableName) {
            return null;
        }

        @Override
        public TransactionContext getTransactionContext() {
            return this.transContext;
        }

        @Override
        protected void startCommit() {

        }

        @Override
        protected void startRollback() {

        }
    }

    public class TransactionContextImpl extends TransactionContext {
        Map<String, String> tableAliases;
        Map<String, Table> tempTables;

        @Override
        public long getTransNum() {
            return 0;
        }

        @Override
        public int getWorkMemSize() {
            return 0;
        }

        @Override
        public void close() {

        }

        @Override
        public String createTempTable(Schema schema) {
            return null;
        }

        @Override
        public void deleteAllTempTables() {

        }

        @Override
        public void setAliasMap(Map<String, String> aliasMap) {

        }

        @Override
        public void clearAliasMap() {

        }

        @Override
        public boolean indexExists(String tableName, String columnName) {
            return false;
        }

        @Override
        public Iterator<java.lang.Record> sortedScan(String tableName, String columnName) {
            return null;
        }

        @Override
        public Iterator<java.lang.Record> sortedScanFrom(String tableName, String columnName, DataBox startValue) {
            return null;
        }

        @Override
        public Iterator<java.lang.Record> lookupKey(String tableName, String columnName, DataBox key) {
            return null;
        }

        @Override
        public BacktrackingIterator<java.lang.Record> getRecordIterator(String tableName) {
            return null;
        }

        @Override
        public boolean contains(String tableName, String columnName, DataBox key) {
            return false;
        }

        @Override
        public RecordId addRecord(String tableName, java.lang.Record record) {
            return null;
        }

        @Override
        public RecordId deleteRecord(String tableName, RecordId rid) {
            return null;
        }

        @Override
        public void deleteRecordWhere(String tableName, String predColumnName, PredicateOperator predOperator, DataBox predValue) {

        }

        @Override
        public void deleteRecordWhere(String tableName, Function<java.lang.Record, DataBox> cond) {

        }

        @Override
        public java.lang.Record getRecord(String tableName, RecordId rid) {
            return null;
        }

        @Override
        public RecordId updateRecord(String tableName, RecordId rid, java.lang.Record updated) {
            return null;
        }

        @Override
        public void updateRecordWhere(String tableName, String targetColumnName, UnaryOperator<DataBox> targetValue, String predColumnName, PredicateOperator predOperator, DataBox predValue) {

        }

        @Override
        public void updateRecordWhere(String tableName, String targetColumnName, Function<java.lang.Record, DataBox> expr, Function<java.lang.Record, DataBox> cond) {

        }

        @Override
        public Schema getSchema(String tableName) {
            return null;
        }

        @Override
        public Schema getFullyQualifiedSchema(String tableName) {
            return null;
        }

        @Override
        public Table getTable(String tableName) {
            return null;
        }

        @Override
        public int getNumDataPages(String tableName) {
            return 0;
        }

        @Override
        public int getTreeOrder(String tableName, String columnName) {
            return 0;
        }

        @Override
        public int getTreeHeight(String tableName, String columnName) {
            return 0;
        }
    }
}
