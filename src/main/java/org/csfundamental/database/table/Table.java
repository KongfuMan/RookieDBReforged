package org.csfundamental.database.table;

import org.csfundamental.database.DatabaseException;
import org.csfundamental.database.Transaction;
import org.csfundamental.database.TransactionContext;
import org.csfundamental.database.buffer.BufferManager;
import org.csfundamental.database.buffer.Page;
import org.csfundamental.database.common.Bits;
import org.csfundamental.database.common.Buffer;
import org.csfundamental.database.common.PredicateOperator;
import org.csfundamental.database.common.iterator.BacktrackingIterable;
import org.csfundamental.database.common.iterator.BacktrackingIterator;
import org.csfundamental.database.common.iterator.IndexBacktrackingIterator;
import org.csfundamental.database.query.QueryPlan;
import org.csfundamental.database.table.databox.DataBox;

import java.util.Iterator;
import java.util.function.Function;
import java.util.function.UnaryOperator;

public class Table implements BacktrackingIterable<Record> {
    private String name;
    private PageDirectory pgDir;
    private Schema schema;
    private int bitmapSizeInBytes;
    private int numRecordsPerPage;

    public Table(String name, PageDirectory pgDir, Schema schema){
        this.name = name;
        this.pgDir = pgDir;
        this.schema = schema;

        this.bitmapSizeInBytes = computeBitmapSizeInBytes(pgDir.getEffectivePageSize(), schema);
        this.numRecordsPerPage = computeNumRecordsPerPage(pgDir.getEffectivePageSize(), schema);
    }

    public String getName() {
        return name;
    }

    public Schema getSchema() {
        return schema;
    }

    public int getNumRecordsPerPage() {
        return numRecordsPerPage;
    }

    // (x + N * size)
    private static int computeBitmapSizeInBytes(int pageSize, Schema schema){
        int recordsPerPage = computeNumRecordsPerPage(pageSize, schema);
        if (recordsPerPage == 1) return 0;
        if (recordsPerPage % 8 == 0) return recordsPerPage / 8;
        return recordsPerPage / 8 + 1;
    }

    public static int computeNumRecordsPerPage(int pageSize, Schema schema) {
        int schemaSize = schema.getSizeInBytes();
        if (schemaSize > pageSize) {
            throw new DatabaseException(String.format(
                    "Schema of size %f bytes is larger than effective page size",
                    schemaSize
            ));
        }
        if (2 * schemaSize + 1 > pageSize) {
            // special case: full page records with no bitmap. Checks if two
            // records + bitmap is larger than the effective page size
            return 1;
        }
        // +1 for space in bitmap
        int recordOverheadInBits = 1 + 8 * schema.getSizeInBytes();
        int pageSizeInBits = pageSize  * 8;
        return pageSizeInBits / recordOverheadInBits;
    }

    /**
     * bit: 0 means free. 1 means occupied
     * */
    private byte[] getBitMap(Page page) {
        if (bitmapSizeInBytes > 0){
            byte[] bitMap = new byte[bitmapSizeInBytes];
            page.getBuffer().position(PageDirectory.DATA_HEADER_SIZE).get(bitMap);
            return bitMap;
        }else{
            return new byte[0xFF];
        }
    }

    private void writeBitMap(Page page, byte[] bitmap) {
        page.pin();
        try{
            page.getBuffer().position(PageDirectory.DATA_HEADER_SIZE).put(bitmap);
        }finally {
            page.unpin();
        }
    }

    private void insertRecord(Record record, int index, Page page){
        int offset = PageDirectory.DATA_HEADER_SIZE + bitmapSizeInBytes + index * schema.getSizeInBytes();
        page.getBuffer().position(offset).put(record.toBytes(schema));
    }

    /**
     * Add a record into the first free slot of the first free
     * page (if one exists, otherwise one is allocated).
     *
     * **/
    public RecordId addRecord(Record record){
        // TODO
        short sizeInBytes = (short)schema.getSizeInBytes();
        Page page = this.pgDir.fetchPageWithSpace(sizeInBytes);
        try{
            byte[] bitMap = getBitMap(page);
            int index = Bits.indexOfFirstZeroBit(bitMap);
            insertRecord(record, index, page);
            Bits.setBit(bitMap, index, Bits.Bit.ONE);
            writeBitMap(page, bitMap);
            return new RecordId(page.getPageNum(), (short)index);
        }finally {
            page.unpin();
        }
    }

    public Record getRecord(RecordId rid){
        Page page = pgDir.fetchPage(rid.getPageNum());
        try{
            byte[] bitMap = getBitMap(page);
            if (Bits.getBit(bitMap, rid.getEntryNum())== Bits.Bit.ZERO){
                throw new DatabaseException("");
            }
            int offset = PageDirectory.DATA_HEADER_SIZE + bitmapSizeInBytes + rid.getEntryNum() * schema.getSizeInBytes();
            Buffer buf = page.getBuffer().position(offset);
            return Record.fromBytes(buf, schema);
        }finally {
            page.unpin();
        }
    }

    public Record updateRecord(RecordId rid, Record record){
        // TODO
        return null;
    }

    public Record deleteRecord(RecordId rid){
        // TODO
        return null;
    }


    /**
     * Iterate through the data pages of this table
     * */
    @Override
    public BacktrackingIterator<Record> iterator() {
        return null;
    }

    public BacktrackingIterator<Page> pageIterator() {
        return pgDir.iterator();
    }

    public BacktrackingIterator<Record> recordIterator(Iterator<RecordId> rids) {
        // TODO(proj4_part2): Update the following line
//        LockUtil.ensureSufficientLockHeld(tableContext, LockType.NL);
        return new RecordIterator(rids);
    }

    /**
     * Iterator over the record id of single page of table
     * */
    class RIDPageIterator extends IndexBacktrackingIterator<RecordId> {
        private Page page;
        private byte[] bitMap;
        public RIDPageIterator(Page page) {
            super(numRecordsPerPage);
            this.page = page;
            this.bitMap = getBitMap(page);
        }

        @Override
        protected int getNextNonEmpty(int currentIndex) {
            for (int i = currentIndex + 1; i < numRecordsPerPage; i++){
                if (Bits.getBit(bitMap, i) == Bits.Bit.ONE){
                    return i;
                }
            }
            return numRecordsPerPage;
        }

        @Override
        protected RecordId getValue(int index) {
            return new RecordId(page.getPageNum(), (short)index);
        }
    }

    /**
     * Iterator over all the data pages of this table.
     * A page of table is an iterator of Record(or RecordId)
     * */
    private class PageIterator implements BacktrackingIterator<BacktrackingIterable<RecordId>> {

        private BacktrackingIterator<Page> sourceIterator;
        private boolean pinOnFetch;

        public PageIterator(boolean pinOnFetch){
            this.sourceIterator = pgDir.iterator();
            this.pinOnFetch = pinOnFetch;
        }

        @Override
        public void markPrev() {
            this.sourceIterator.markPrev();
        }

        @Override
        public void markNext() {
            this.sourceIterator.markNext();
        }

        @Override
        public void reset() {
            this.sourceIterator.reset();
        }

        @Override
        public boolean hasNext() {
            return this.sourceIterator.hasNext();
        }

        @Override
        public BacktrackingIterable<RecordId> next() {
            Page nextPage = this.sourceIterator.next();
            return new InnerIterable(nextPage);
        }

        private class InnerIterable implements BacktrackingIterable<RecordId> {
            private Page page;
            public  InnerIterable(Page page){

            }
            @Override
            public BacktrackingIterator<RecordId> iterator() {
                return new RIDPageIterator(page);
            }
        }
    }

    /**
     * Wraps the RIDPageIterator and PageIterator to form an iterator
     * over all the records of this table.
     * */
    private class RecordIterator implements BacktrackingIterator<Record> {
        private Iterator<RecordId> ridIterator;

        public RecordIterator(Iterator<RecordId> ridIterator){
            this.ridIterator = ridIterator;
        }

        @Override
        public void markPrev() {
            if (ridIterator instanceof BacktrackingIterator) {
                ((BacktrackingIterator<RecordId>) ridIterator).markPrev();
            } else {
                throw new UnsupportedOperationException("Cannot markPrev using underlying iterator");
            }
        }

        @Override
        public void markNext() {
            if (ridIterator instanceof BacktrackingIterator) {
                ((BacktrackingIterator<RecordId>) ridIterator).markNext();
            } else {
                throw new UnsupportedOperationException("Cannot markNext using underlying iterator");
            }
        }

        @Override
        public void reset() {
            if (ridIterator instanceof BacktrackingIterator) {
                ((BacktrackingIterator<RecordId>) ridIterator).reset();
            } else {
                throw new UnsupportedOperationException("Cannot reset using underlying iterator");
            }
        }

        @Override
        public boolean hasNext() {
            return this.ridIterator.hasNext();
        }

        @Override
        public Record next() {
            try{
                return getRecord(this.ridIterator.next());
            } catch (DatabaseException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public class TransactionImpl extends Transaction{
        @Override
        public long getTransNum() {
            return 0;
        }

        @Override
        public void cleanup() {

        }

        @Override
        public void createTable(Schema s, String tableName) {

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
        public QueryPlan query(String tableName, String alias) {
            return null;
        }

        @Override
        public void insert(String tableName, java.lang.Record record) {

        }

        @Override
        public void update(String tableName, String targetColumnName, UnaryOperator<DataBox> targetValue) {

        }

        @Override
        public void update(String tableName, String targetColumnName, UnaryOperator<DataBox> targetValue, String predColumnName, PredicateOperator predOperator, DataBox predValue) {

        }

        @Override
        public void update(String tableName, String targetColumnName, Function<java.lang.Record, DataBox> expr, Function<java.lang.Record, DataBox> cond) {

        }

        @Override
        public void delete(String tableName, String predColumnName, PredicateOperator predOperator, DataBox predValue) {

        }

        @Override
        public void delete(String tableName, Function<java.lang.Record, DataBox> cond) {

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
            return null;
        }

        @Override
        protected void startCommit() {

        }

        @Override
        protected void startRollback() {

        }
    }

}
