package org.csfundamental.database.storage;

import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import static org.csfundamental.database.storage.DiskSpaceManagerImpl.DATA_PAGES_PER_HEADER;
import static org.csfundamental.database.storage.DiskSpaceManagerImpl.HEADER_PAGES_PER_MASTER;
import static org.csfundamental.database.storage.IDiskSpaceManager.PAGE_SIZE;

public class PartitionTest {
    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();
    private static final String EMPTY_PARTITION = "empty_partition";
    private File emptyPartFile;
    private String emptyPartPath;
    @Before
    public void beforeEach() throws IOException {
        emptyPartFile = tmpFolder.newFile(EMPTY_PARTITION);
        emptyPartPath = emptyPartFile.getAbsolutePath();
    }
    @Test
    public void alloc_DataPages_Under_FirstNHeaderPage() throws Exception {
        Partition part1 = new Partition(0);
        part1.open(emptyPartPath);
        for (int hIdx = 0; hIdx < HEADER_PAGES_PER_MASTER; hIdx += 32){
            for (int dIdx = 0; dIdx < DATA_PAGES_PER_HEADER; dIdx += DATA_PAGES_PER_HEADER - 1){
                part1.allocPage(hIdx, dIdx);
                Partition part2 = new Partition(0);
                part2.open(emptyPartPath);
                comparePartition(part1, part2);
            }
        }
    }
    @Test
    public void dealloc_DataPages() throws Exception {
        Partition part1 = new Partition(0);
        part1.open(emptyPartPath);
        int allocCount = 10;
        for (int i = 0; i < allocCount; i++){
            int pageNum = part1.allocPage();
            Assert.assertEquals(pageNum, i);
            Partition part2 = new Partition(0);
            part2.open(emptyPartPath);
            comparePartition(part1, part2);
        }

        for (int i = 0; i < allocCount - 1; i++){
            part1.freePage(i);
            Partition part2 = new Partition(0);
            part2.open(emptyPartPath);
            comparePartition(part1, part2);
        }

        part1.freePage(allocCount - 1);
        Partition part2 = new Partition(0);
        part2.open(emptyPartPath);
        int[] master = part2.getMasterPage();
        Assert.assertArrayEquals(master, new int[master.length]);
        byte[][] headers = part2.getHeaderPages();
        for (int i = 0; i < headers.length; i++){
            Assert.assertNull(headers[i]);
        }
    }
    @Test
    public void free_AllDataPages() throws IOException {
        Partition part1 = new Partition(0);
        part1.open(emptyPartPath);
        int allocCount = 10;
        for (int i = 0; i < allocCount; i++){
            part1.allocPage();
        }
        part1.freeDataPages();

        Partition part2 = new Partition(0);
        part2.open(emptyPartPath);
        int[] master = part2.getMasterPage();
        Assert.assertArrayEquals(master, new int[master.length]);
        byte[][] headers = part2.getHeaderPages();
        for (int i = 0; i < headers.length; i++){
            Assert.assertNull(headers[i]);
        }
    }
    @Test(expected = PageException.class)
    public void writeUnallocatedPage() throws IOException {
        Partition part1 = new Partition(0);
        part1.open(emptyPartPath);
        byte[] buf = new byte[PAGE_SIZE];
        part1.writePage(0, buf);
    }
    @Test(expected = PageException.class)
    public void readUnallocatedPage() throws IOException {
        Partition part = new Partition(0);
        part.open(emptyPartPath);
        byte[] buf = new byte[PAGE_SIZE];
        part.readPage(0, buf);
    }
    @Test
    public void RWAllocatedPage() throws IOException {
        Partition part = new Partition(0);
        part.open(emptyPartPath);
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
        Assert.assertArrayEquals(part1.getMasterPage(), part1.getMasterPage());
        byte[][] headerPages1 = part1.getHeaderPages();
        byte[][] headerPages2 = part2.getHeaderPages();
        Assert.assertEquals(headerPages1.length, headerPages2.length);
        for (int i = 0; i < headerPages1.length; i++){
            Assert.assertArrayEquals(headerPages1[i], headerPages2[i]);
        }
    }
}