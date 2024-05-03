package org.csfundamental.database.storage;

import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.UUID;

import static org.csfundamental.database.storage.DiskSpaceManager.PAGE_SIZE;
import static org.csfundamental.database.storage.DiskSpaceManagerImpl.DATA_PAGES_PER_HEADER;
import static org.csfundamental.database.storage.DiskSpaceManagerImpl.HEADER_PAGES_PER_MASTER;
import static org.junit.Assert.*;

public class PartitionTest {
    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();
    private static final String PARTITION_NAME = "test_partition";
    private String partPath;

    @Before
    public void beforeEach() throws IOException {
        File partFile = tmpFolder.newFile(PARTITION_NAME);
        partPath = partFile.getAbsolutePath();
    }

    @Test
    public void alloc_DataPages_Under_FirstNHeaderPage() throws Exception {
        Partition part1 = new Partition(0);
        part1.loadFromFile(partPath);
        // hIdx and dIdx in for loop don't increment by 1 for performance concern.
        for (int hIdx = 0; hIdx < HEADER_PAGES_PER_MASTER; hIdx += 32){
            int inc = hIdx == 0 ? 1 : DATA_PAGES_PER_HEADER - 1;
            for (int dIdx = 0; dIdx < DATA_PAGES_PER_HEADER; dIdx += inc){
                int pageNum = part1.allocPage();
                Partition part2 = new Partition(1);
                part2.loadFromFile(partPath);   // load from file
                comparePartition(part1, part2);
                assertFalse(part2.isFreePage(pageNum));
            }
        }
    }

    @Test
    public void dealloc_DataPages() throws Exception {
        Partition part1 = new Partition(0);
        part1.loadFromFile(partPath);
        int allocCount = 100;
        for (int i = 0; i < allocCount; i++){
            int pageNum = part1.allocPage();
            Assert.assertEquals(pageNum, i);
            Partition part2 = new Partition(1);
            part2.loadFromFile(partPath);
            comparePartition(part1, part2);
            assertFalse(part2.isFreePage(pageNum));
        }

        for (int i = 0; i < allocCount - 1; i++){
            part1.freePage(i);
            Partition part2 = new Partition(1);
            part2.loadFromFile(partPath);
            comparePartition(part1, part2);
            assertTrue(part2.isFreePage(i));
        }

        part1.freePage(allocCount - 1);
        Partition part2 = new Partition(1);
        part2.loadFromFile(partPath);
        int[] master = part2.getMasterPage();
        Assert.assertArrayEquals(master, new int[master.length]);
        BitSet[] headers = part2.getHeaderPages();
        for (int i = 0; i < headers.length; i++){
            Assert.assertEquals(0, headers[i].cardinality());
        }
    }

    @Test
    public void free_AllDataPages() throws IOException {
        Partition part1 = new Partition(0);
        part1.loadFromFile(partPath);
        int allocCount = 10;
        for (int i = 0; i < allocCount; i++){
            part1.allocPage();
        }
        part1.freeAllPages();

        Partition part2 = new Partition(0);
        part2.loadFromFile(partPath);
        int[] master = part2.getMasterPage();
        Assert.assertArrayEquals(master, new int[master.length]);
        BitSet[] headers = part2.getHeaderPages();
        for (int i = 0; i < headers.length; i++){
            Assert.assertEquals(0, headers[i].cardinality());
        }
    }

    @Test(expected = PageException.class)
    public void writeUnallocatedPage() throws IOException {
        Partition part1 = new Partition(0);
        part1.loadFromFile(partPath);
        byte[] buf = new byte[PAGE_SIZE];
        part1.writePage(0, buf);
    }

    @Test(expected = PageException.class)
    public void readUnallocatedPage() throws IOException {
        Partition part = new Partition(0);
        part.loadFromFile(partPath);
        byte[] buf = new byte[PAGE_SIZE];
        part.readPage(0, buf);
    }

    @Test
    public void RWAllocatedPage() throws IOException {
        Partition part = new Partition(0);
        part.loadFromFile(partPath);
        int pageNum = part.allocPage();
        byte[] wBuf = new byte[PAGE_SIZE];
        ByteBuffer buf = ByteBuffer.wrap(wBuf);
        UUID id = UUID.randomUUID();
        for (char ch : id.toString().toCharArray()){
            buf.putChar(ch);
        }
        part.writePage(pageNum, wBuf);

        byte[] rBuf = new byte[PAGE_SIZE];
        part.readPage(pageNum, rBuf);
        Assert.assertArrayEquals(wBuf, rBuf);
    }

    private void comparePartition(Partition part1, Partition part2){
        Assert.assertArrayEquals(part1.getMasterPage(), part2.getMasterPage());
        BitSet[] headerPages1 = part1.getHeaderPages();
        BitSet[] headerPages2 = part2.getHeaderPages();
        Assert.assertEquals(headerPages1.length, headerPages2.length);
        for (int i = 0; i < headerPages1.length; i++){
            Assert.assertEquals(headerPages1[i], headerPages2[i]);
        }
    }
}