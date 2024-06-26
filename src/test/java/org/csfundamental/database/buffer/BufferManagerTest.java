package org.csfundamental.database.buffer;

import org.csfundamental.database.common.Buffer;
import org.csfundamental.database.storage.DiskSpaceManager;
import org.csfundamental.database.storage.MockDiskSpaceManager;
import org.csfundamental.database.storage.PageException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static org.csfundamental.database.storage.DiskSpaceManager.PAGE_SIZE;
import static org.junit.Assert.*;

public class BufferManagerTest {
    private DiskSpaceManager diskSpaceManager;
    private BufferManager bufferManager;

    private byte[] generateRandomByteArray(){
        byte[] data = new byte[PAGE_SIZE];
        Random rand = new Random();
        rand.nextBytes(data);
        return data;
    }

    @Before
    public void beforeEach() {
        diskSpaceManager = new MockDiskSpaceManager();
        bufferManager = new BufferManager(diskSpaceManager, 5);
    }

    @After
    public void afterEach() {
        bufferManager.close();
        diskSpaceManager.close();
    }

    @Test
    public void validateMemoryDiskSpaceManager(){
        diskSpaceManager.allocPart(0);
        byte[] rData = new byte[PAGE_SIZE];
        for (long i = 0L; i < 10L; i++){
            byte[] wData = generateRandomByteArray();
            diskSpaceManager.allocPage(i);
            diskSpaceManager.writePage(i, wData);
            Arrays.fill(rData, (byte)0);
            diskSpaceManager.readPage(i, rData);
            Assert.assertArrayEquals(wData, rData);
        }
    }

    @Test
    public void testFetchNewPage() {
        int partNum = diskSpaceManager.allocPart(1);

        BufferFrame frame1 = bufferManager.fetchNewPageFrame(partNum);
        BufferFrame frame2 = bufferManager.fetchNewPageFrame(partNum);
        BufferFrame frame3 = bufferManager.fetchNewPageFrame(partNum);

        frame1.unpin();
        frame2.unpin();
        frame3.unpin();

        Assert.assertTrue(frame1.isValid());
        Assert.assertTrue(frame2.isValid());
        Assert.assertTrue(frame3.isValid());

        BufferFrame frame4 = bufferManager.fetchNewPageFrame(partNum);
        BufferFrame frame5 = bufferManager.fetchNewPageFrame(partNum);
        BufferFrame frame6 = bufferManager.fetchNewPageFrame(partNum);
        frame4.unpin();
        frame5.unpin();
        frame6.unpin();

        assertFalse(frame1.isValid());
        assertTrue(frame2.isValid());
        assertTrue(frame3.isValid());
        assertTrue(frame4.isValid());
        assertTrue(frame5.isValid());
        assertTrue(frame6.isValid());
    }

    @Test
    public void testFetchPage(){
        int partNum = diskSpaceManager.allocPart(1);

        BufferFrame frame1 = bufferManager.fetchNewPageFrame(partNum);
        BufferFrame frame2 = bufferManager.fetchPageFrame(frame1.getPageNum());

        frame1.unpin();
        frame2.unpin();

        Assert.assertSame(frame1, frame2);
    }

    @Test
    public void testReadWrite(){
        int partNum = diskSpaceManager.allocPart(1);

        byte[] expected = new byte[] { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF };
        byte[] actual = new byte[4];

        BufferFrame frame1 = bufferManager.fetchNewPageFrame(partNum);
        frame1.writeBytes((short) 67, (short) 4, expected);
        frame1.readBytes((short) 67, (short) 4, actual);
        frame1.unpin();

        Assert.assertArrayEquals(expected, actual);
    }

    @Test
    public void testFlush(){
        int partNum = diskSpaceManager.allocPart(1);

        byte[] expected = new byte[] { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF };
        byte[] actual = new byte[DiskSpaceManager.PAGE_SIZE];

        BufferFrame frame1 = bufferManager.fetchNewPageFrame(partNum);
        frame1.writeBytes((short) 67, (short) 4, expected);
        frame1.unpin();

        // not flushed yet
        diskSpaceManager.readPage(frame1.getPageNum(), actual);
        assertArrayEquals(new byte[4], Arrays.copyOfRange(actual, 67 + BufferManager.RESERVED_SPACE,
                71 + BufferManager.RESERVED_SPACE));

        // explicitly flushed
        frame1.flush();
        diskSpaceManager.readPage(frame1.getPageNum(), actual);
        assertArrayEquals(expected, Arrays.copyOfRange(actual, 67 + BufferManager.RESERVED_SPACE,
                71 + BufferManager.RESERVED_SPACE));

        frame1.pin();
        frame1.writeBytes((short) 33, (short) 4, expected);
        frame1.unpin();
        diskSpaceManager.readPage(frame1.getPageNum(), actual);

        assertArrayEquals(new byte[4], Arrays.copyOfRange(actual, 33 + BufferManager.RESERVED_SPACE,
                37 + BufferManager.RESERVED_SPACE));

        // force an eviction by reaching cache capacity: 5
        BufferFrame frame2 = bufferManager.fetchNewPageFrame(partNum);
        frame2.unpin();
        BufferFrame frame3 = bufferManager.fetchNewPageFrame(partNum);
        frame3.unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();

        diskSpaceManager.readPage(frame1.getPageNum(), actual);
        assertFalse(frame1.isValid());
        assertArrayEquals(expected, Arrays.copyOfRange(actual, 33 + BufferManager.RESERVED_SPACE,
                37 + BufferManager.RESERVED_SPACE));

        // non-dirty frame2
        diskSpaceManager.readPage(frame2.getPageNum(), actual);
        assertArrayEquals(new byte[4], Arrays.copyOfRange(actual, 33 + BufferManager.RESERVED_SPACE,
                37 + BufferManager.RESERVED_SPACE));
    }

    @Test
    public void testReload(){
        int partNum = diskSpaceManager.allocPart(1);

        byte[] expected = new byte[] { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF };
        byte[] actual = new byte[4];

        BufferFrame frame1 = bufferManager.fetchNewPageFrame(partNum);
        frame1.writeBytes((short) 67, (short) 4, expected);
        frame1.unpin();

        // force a eviction
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();

        assertFalse(frame1.isValid());

        // reload page
        frame1 = bufferManager.fetchPageFrame(frame1.getPageNum());
        frame1.readBytes((short) 67, (short) 4, actual);
        frame1.unpin();

        assertArrayEquals(expected, actual);
    }

    @Test
    public void testFreePage() {
        int partNum = diskSpaceManager.allocPart(1);

        byte[] expected = new byte[] { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF };
        byte[] actual = new byte[4];

        BufferFrame frame1 = bufferManager.fetchNewPageFrame(partNum);
        BufferFrame frame2 = bufferManager.fetchNewPageFrame(partNum);
        Page page3 = bufferManager.fetchNewPage(partNum);
        BufferFrame frame4 = bufferManager.fetchNewPageFrame(partNum);
        BufferFrame frame5 = bufferManager.fetchNewPageFrame(partNum);

        frame1.unpin();
        frame2.unpin();
        frame4.unpin();
        frame5.unpin();

        bufferManager.freePage(page3);

        try {
            diskSpaceManager.readPage(page3.getPageNum(), new byte[DiskSpaceManager.PAGE_SIZE]);
            fail();
        } catch (PageException e) { /* do nothing */ }

        BufferFrame frame6  = bufferManager.fetchNewPageFrame(partNum);
        frame6.unpin();
        assertTrue(frame1.isValid());
        assertTrue(frame2.isValid());
        assertTrue(frame4.isValid());
        assertTrue(frame5.isValid());
        assertTrue(frame6.isValid());
    }

    @Test
    public void freePart() {
        int partNum1 = diskSpaceManager.allocPart(1);
        int partNum2 = diskSpaceManager.allocPart(2);

        BufferFrame frame1 = bufferManager.fetchNewPageFrame(partNum1);
        BufferFrame frame2 = bufferManager.fetchNewPageFrame(partNum2);
        BufferFrame frame3 = bufferManager.fetchNewPageFrame(partNum1);
        BufferFrame frame4 = bufferManager.fetchNewPageFrame(partNum2);
        BufferFrame frame5 = bufferManager.fetchNewPageFrame(partNum2);

        frame1.unpin();
        frame2.unpin();
        frame3.unpin();
        frame4.unpin();
        frame5.unpin();

        bufferManager.freePart(partNum1);

        try {
            diskSpaceManager.readPage(frame1.getPageNum(), new byte[DiskSpaceManager.PAGE_SIZE]);
            fail();
        } catch (Exception e) { /* do nothing */ }
        try {
            diskSpaceManager.readPage(frame3.getPageNum(), new byte[DiskSpaceManager.PAGE_SIZE]);
            fail();
        } catch (Exception e) { /* do nothing */ }
        try {
            diskSpaceManager.allocPage(partNum1);
            fail();
        } catch (Exception e) { /* do nothing */ }

        BufferFrame frame6  = bufferManager.fetchNewPageFrame(partNum2);
        BufferFrame frame7  = bufferManager.fetchNewPageFrame(partNum2);
        frame6.unpin();
        frame7.unpin();
        assertFalse(frame1.isValid());
        assertTrue(frame2.isValid());
        assertFalse(frame3.isValid());
        assertTrue(frame4.isValid());
        assertTrue(frame5.isValid());
        assertTrue(frame6.isValid());
        assertTrue(frame7.isValid());
    }

    @Test(expected = PageException.class)
    public void testMissingPart() {
        bufferManager.fetchPageFrame(DiskSpaceManager.getVirtualPageNum(0, 0));
    }

    @Test(expected = PageException.class)
    public void testMissingPage() {
        int partNum = diskSpaceManager.allocPart(1);
        bufferManager.fetchPageFrame(DiskSpaceManager.getVirtualPageNum(partNum, 0));
    }

    @Test
    public void testReadWriteVariousDataType(){
        int partNum = diskSpaceManager.allocPart(1);
        Page page = bufferManager.fetchNewPage(partNum);
        Buffer pageBuffer = page.getBuffer();

        Random rand = new Random();
        int val1 = rand.nextInt();
        int val2 = rand.nextInt();
        char ch = 'a';
        float f = rand.nextFloat();

        pageBuffer.putInt(val1)
                .putInt(val2)
                .putChar(ch)
                .putFloat(f);

        pageBuffer = page.getBuffer();
        Assert.assertEquals(val1, pageBuffer.getInt());
        Assert.assertEquals(val2, pageBuffer.getInt());
        Assert.assertEquals(ch, pageBuffer.getChar());
        Assert.assertEquals(f, pageBuffer.getFloat(), 0.000000000);
    }
}