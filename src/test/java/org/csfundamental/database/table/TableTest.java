//package org.csfundamental.database.table;
//
//import org.csfundamental.database.DatabaseException;
//import org.csfundamental.database.TestUtils;
//import org.csfundamental.database.buffer.BufferManager;
//import org.csfundamental.database.buffer.Page;
//import org.csfundamental.database.common.iterator.BacktrackingIterator;
//import org.csfundamental.database.storage.DiskSpaceManager;
//import org.csfundamental.database.storage.MemoryDiskSpaceManager;
//import org.csfundamental.database.table.databox.Type;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Rule;
//import org.junit.Test;
//import org.junit.rules.TemporaryFolder;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import static org.junit.Assert.*;
//
//public class TableTest {
//    private static final String TABLENAME = "testtable";
//    private PageDirectory pageDirectory;
//    private Table table;
//    private Schema schema;
//    private BufferManager bufferManager;
//    @Rule
//    public TemporaryFolder fileDir = new TemporaryFolder();
//
//    @Before
//    public void setup(){
//        int partNum = 1;
//        DiskSpaceManager diskSpaceManager = new MemoryDiskSpaceManager();
//        diskSpaceManager.allocPart(partNum);
//        this.bufferManager = new BufferManager(diskSpaceManager, 1024);
//        Page page = this.bufferManager.fetchNewPage(partNum);
//        this.pageDirectory = new PageDirectory(this.bufferManager, partNum, page.getPageNum());
//        this.schema = TestUtils.createSchemaWithAllTypes();
//        this.table = new Table(TABLENAME, this.pageDirectory, this.schema);
//    }
//
//    @Test
//    public void testGetNumRecordsPerPage(){
//        assertEquals(785, schema.getSizeInBytes());
//        assertEquals(4050, this.pageDirectory.getEffectivePageSize());
//        // bitmap size + records * recordSize
//        // 50 + (400 * 10) = 4050
//        // 51 + (408 * 10) = 4131
//        assertEquals(5, table.getNumRecordsPerPage());
//    }
//
//    @Test
//    public void testSingleInsertAndGet() {
//        Record expected = TestUtils.createRecordWithAllTypes();
//        RecordId rid = table.addRecord(expected);
//        Record actual = table.getRecord(rid);
//        assertEquals(expected, actual);
//    }
//
//    @Test
//    public void testThreePagesOfInserts() {
//        List<RecordId> rids = new ArrayList<>();
//        List<Record> records = new ArrayList<>();
//        for (int i = 0; i < table.getNumRecordsPerPage() * 3; ++i) {
//            Record r = TestUtils.createRecordWithAllTypes(i);
//            records.add(r);
//            rids.add(table.addRecord(r));
//        }
//
//        for (int i = 0; i < table.getNumRecordsPerPage() * 3; ++i) {
//            Record expectedRecord = records.get(i);
//            Record actualRecord = table.getRecord(rids.get(i));
//            Assert.assertEquals(expectedRecord, actualRecord);
//        }
//    }
//
//    @Test
//    public void testManyPagesOfInserts() {
//        List<RecordId> rids = new ArrayList<>();
//        List<Record> records = new ArrayList<>();
//        int numPages = 100000;
//        for (int i = 0; i < table.getNumRecordsPerPage() * numPages; ++i) {
//            Record r = TestUtils.createRecordWithAllTypes(i);
//            records.add(r);
//            rids.add(table.addRecord(r));
//        }
//
//        for (int i = 0; i < table.getNumRecordsPerPage() * numPages; ++i) {
//            Record expectedRecord = records.get(i);
//            Record actualRecord = table.getRecord(rids.get(i));
//            Assert.assertEquals(expectedRecord, actualRecord);
//        }
//    }
//
//    /**
//     * Basic test over a full page of records to check that next/hasNext work.
//     */
//    @Test
//    public void testRIDPageIterator() throws DatabaseException {
//        List<Record> records = new ArrayList<>();
//        for (int i = 0; i < table.getNumRecordsPerPage() * 3; ++i) {
//            Record record = TestUtils.createRecordWithAllTypes(i);
//            records.add(record);
//            table.addRecord(record);
//        }
//        BacktrackingIterator<Page> pgIterator = table.pageIterator();
//        Page page = pgIterator.next();
//
//        BacktrackingIterator<Record> iter = table.recordIterator(table.new RIDPageIterator(page));
//        int i = 0;
//        while(iter.hasNext()){
//            Record actualRecord = iter.next();
//            Record expectedRecord = records.get(i++);
//            Assert.assertEquals(actualRecord, expectedRecord);
//        }
//    }
//}