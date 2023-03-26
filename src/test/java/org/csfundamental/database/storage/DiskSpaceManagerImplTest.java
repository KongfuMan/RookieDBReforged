package org.csfundamental.database.storage;

import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class DiskSpaceManagerImplTest {
    private static final String DIR = "diskspacemanager";
    private String dirPath;
    private File dsmRootDir;
    private Path dsmRootPath;
    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Before
    public void beforeEach() throws IOException {
        dsmRootDir = tmpFolder.newFolder(DIR);
        dsmRootPath = dsmRootDir.toPath();
        dirPath = dsmRootDir.getAbsolutePath();
    }

    private DiskSpaceManager createDiskSpaceManager() {
        return new DiskSpaceManagerImpl(dirPath);
    }

    @Test
    public void allocPartitionsWithMultipleThread() throws InterruptedException {
        DiskSpaceManager dsm = createDiskSpaceManager();
        int nThread = 10;
        int allocCount = 30;
        Thread[] allocPartWorkers = new Thread[nThread];
        for (int i = 0; i < allocPartWorkers.length; i++){
            allocPartWorkers[i] = new Thread(()->{
                for (int j = 0; j < allocCount; j++){
                    dsm.allocPart();
                }
            });
        }
        for (Thread worker : allocPartWorkers){
            worker.start();
        }

        for (Thread worker: allocPartWorkers){
            worker.join();
        }

        for (int i = 0; i < nThread * allocCount; i++){
            Assert.assertTrue(dsmRootPath.resolve(String.valueOf(i)).toFile().exists());
        }

        Assert.assertEquals(nThread * allocCount, dsm.getCurrentPartNum());
    }

    @Test(expected = NoSuchElementException.class)
    public void freeUnallocatedPart() throws IOException {
        DiskSpaceManager dsm = createDiskSpaceManager();
        dsm.freePart(0);
    }

    @Test
    public void testFreePart() throws IOException {
        DiskSpaceManager dsm = createDiskSpaceManager();
        int partNum = dsm.allocPart();
        Assert.assertEquals(0, partNum);
        Assert.assertTrue(dsmRootPath.resolve(String.valueOf(partNum)).toFile().exists());

        dsm.freePart(partNum);
        Assert.assertFalse(dsmRootPath.resolve(String.valueOf(partNum)).toFile().exists());
    }

    @Test
    public void testAllocPageZeroed() throws IOException {
        DiskSpaceManager dsm = createDiskSpaceManager();
        int partNum = dsm.allocPart(0);
        long pageNum1 = dsm.allocPage(0);
        long pageNum2 = dsm.allocPage(partNum);

        assertEquals(0L, pageNum1);
        assertEquals(1L, pageNum2);

        byte[] buf = new byte[DiskSpaceManager.PAGE_SIZE];
        dsm.readPage(pageNum1, buf);
        assertArrayEquals(new byte[DiskSpaceManager.PAGE_SIZE], buf);
        dsm.readPage(pageNum2, buf);
        assertArrayEquals(new byte[DiskSpaceManager.PAGE_SIZE], buf);

        long pageNum3 = dsm.allocPage(partNum);
        long pageNum4 = dsm.allocPage(partNum);
        dsm.close();

        dsm = createDiskSpaceManager();
        dsm.readPage(pageNum1, buf);
        assertArrayEquals(new byte[DiskSpaceManager.PAGE_SIZE], buf);
        dsm.readPage(pageNum2, buf);
        assertArrayEquals(new byte[DiskSpaceManager.PAGE_SIZE], buf);
        dsm.readPage(pageNum3, buf);
        assertArrayEquals(new byte[DiskSpaceManager.PAGE_SIZE], buf);
        dsm.readPage(pageNum4, buf);
        assertArrayEquals(new byte[DiskSpaceManager.PAGE_SIZE], buf);

        dsm.close();
    }

    @Test(expected = NoSuchElementException.class)
    public void testReadBadPart() throws IOException {
        DiskSpaceManager dsm = createDiskSpaceManager();
        dsm.readPage(0, new byte[DiskSpaceManager.PAGE_SIZE]);
        dsm.close();
    }

    @Test(expected = NoSuchElementException.class)
    public void testWriteBadPart() throws IOException {
        DiskSpaceManager dsm = createDiskSpaceManager();
        dsm.writePage(0, new byte[DiskSpaceManager.PAGE_SIZE]);
        dsm.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReadBadBuffer() throws IOException {
        DiskSpaceManager dsm = createDiskSpaceManager();
        dsm.readPage(0, new byte[DiskSpaceManager.PAGE_SIZE - 1]);
        dsm.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteBadBuffer() throws IOException {
        DiskSpaceManager dsm = createDiskSpaceManager();
        dsm.writePage(0, new byte[DiskSpaceManager.PAGE_SIZE + 1]);
        dsm.close();
    }

    @Test(expected = PageException.class)
    public void testReadOutOfBounds() throws IOException {
        DiskSpaceManager dsm = createDiskSpaceManager();
        dsm.allocPart();
        dsm.readPage(0, new byte[DiskSpaceManager.PAGE_SIZE]);
        dsm.close();
    }

    @Test(expected = PageException.class)
    public void testWriteOutOfBounds() throws IOException {
        DiskSpaceManager dsm = createDiskSpaceManager();
        dsm.allocPart();
        dsm.writePage(0, new byte[DiskSpaceManager.PAGE_SIZE]);
        dsm.close();
    }

    @Test
    public void testReadWrite() throws IOException {
        DiskSpaceManager dsm = createDiskSpaceManager();
        int partNum = dsm.allocPart();
        long pageNum = dsm.allocPage(partNum);

        byte[] buf = new byte[DiskSpaceManager.PAGE_SIZE];
        for (int i = 0; i < buf.length; ++i) {
            buf[i] = (byte) (Integer.valueOf(i).hashCode() & 0xFF);
        }
        dsm.writePage(pageNum, buf);
        byte[] readbuf = new byte[DiskSpaceManager.PAGE_SIZE];
        dsm.readPage(pageNum, readbuf);

        assertArrayEquals(buf, readbuf);

        dsm.freePart(partNum);
        dsm.close();
    }

    @Test
    public void testReadWritePersistent() throws IOException {
        DiskSpaceManager diskSpaceManager = createDiskSpaceManager();
        int partNum = diskSpaceManager.allocPart();
        long pageNum = diskSpaceManager.allocPage(partNum);

        byte[] buf = new byte[DiskSpaceManager.PAGE_SIZE];
        for (int i = 0; i < buf.length; ++i) {
            buf[i] = (byte) (Integer.valueOf(i).hashCode() & 0xFF);
        }
        diskSpaceManager.writePage(pageNum, buf);
        diskSpaceManager.close();

        diskSpaceManager = createDiskSpaceManager();
        byte[] readbuf = new byte[DiskSpaceManager.PAGE_SIZE];
        diskSpaceManager.readPage(pageNum, readbuf);

        assertArrayEquals(buf, readbuf);

        diskSpaceManager.freePart(partNum);
        diskSpaceManager.close();
    }

    @Test
    public void testReadWriteMultiplePartitions() throws IOException {
        DiskSpaceManager diskSpaceManager = createDiskSpaceManager();
        int partNum1 = diskSpaceManager.allocPart();
        int partNum2 = diskSpaceManager.allocPart();
        long pageNum11 = diskSpaceManager.allocPage(partNum1);
        long pageNum21 = diskSpaceManager.allocPage(partNum2);
        long pageNum22 = diskSpaceManager.allocPage(partNum2);

        byte[] buf1 = new byte[DiskSpaceManager.PAGE_SIZE];
        byte[] buf2 = new byte[DiskSpaceManager.PAGE_SIZE];
        byte[] buf3 = new byte[DiskSpaceManager.PAGE_SIZE];
        for (int i = 0; i < buf1.length; ++i) {
            buf1[i] = (byte) (Integer.valueOf(i).hashCode() & 0xFF);
            buf2[i] = (byte) ((Integer.valueOf(i).hashCode() >> 8) & 0xFF);
            buf3[i] = (byte) ((Integer.valueOf(i).hashCode() >> 16) & 0xFF);
        }
        diskSpaceManager.writePage(pageNum11, buf1);
        diskSpaceManager.writePage(pageNum22, buf3);
        diskSpaceManager.writePage(pageNum21, buf2);
        byte[] readbuf1 = new byte[DiskSpaceManager.PAGE_SIZE];
        byte[] readbuf2 = new byte[DiskSpaceManager.PAGE_SIZE];
        byte[] readbuf3 = new byte[DiskSpaceManager.PAGE_SIZE];
        diskSpaceManager.readPage(pageNum11, readbuf1);
        diskSpaceManager.readPage(pageNum21, readbuf2);
        diskSpaceManager.readPage(pageNum22, readbuf3);

        assertArrayEquals(buf1, readbuf1);
        assertArrayEquals(buf2, readbuf2);
        assertArrayEquals(buf3, readbuf3);

        diskSpaceManager.freePart(partNum1);
        diskSpaceManager.freePart(partNum2);
        diskSpaceManager.close();
    }

    @Test
    public void testReadWriteMultiplePartitionsPersistent() throws IOException {
        DiskSpaceManager diskSpaceManager = createDiskSpaceManager();
        int partNum1 = diskSpaceManager.allocPart();
        int partNum2 = diskSpaceManager.allocPart();
        long pageNum11 = diskSpaceManager.allocPage(partNum1);
        long pageNum21 = diskSpaceManager.allocPage(partNum2);
        long pageNum22 = diskSpaceManager.allocPage(partNum2);

        byte[] buf1 = new byte[DiskSpaceManager.PAGE_SIZE];
        byte[] buf2 = new byte[DiskSpaceManager.PAGE_SIZE];
        byte[] buf3 = new byte[DiskSpaceManager.PAGE_SIZE];
        for (int i = 0; i < buf1.length; ++i) {
            buf1[i] = (byte) (Integer.valueOf(i).hashCode() & 0xFF);
            buf2[i] = (byte) ((Integer.valueOf(i).hashCode() >> 8) & 0xFF);
            buf3[i] = (byte) ((Integer.valueOf(i).hashCode() >> 16) & 0xFF);
        }
        diskSpaceManager.writePage(pageNum11, buf1);
        diskSpaceManager.writePage(pageNum22, buf3);
        diskSpaceManager.writePage(pageNum21, buf2);
        diskSpaceManager.close();

        diskSpaceManager = createDiskSpaceManager();
        byte[] readbuf1 = new byte[DiskSpaceManager.PAGE_SIZE];
        byte[] readbuf2 = new byte[DiskSpaceManager.PAGE_SIZE];
        byte[] readbuf3 = new byte[DiskSpaceManager.PAGE_SIZE];
        diskSpaceManager.readPage(pageNum11, readbuf1);
        diskSpaceManager.readPage(pageNum21, readbuf2);
        diskSpaceManager.readPage(pageNum22, readbuf3);

        assertArrayEquals(buf1, readbuf1);
        assertArrayEquals(buf2, readbuf2);
        assertArrayEquals(buf3, readbuf3);

        diskSpaceManager.freePart(partNum1);
        diskSpaceManager.freePart(partNum2);
        diskSpaceManager.close();
    }
}