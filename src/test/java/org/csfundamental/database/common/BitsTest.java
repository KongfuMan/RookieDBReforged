package org.csfundamental.database.common;

import org.junit.Assert;
import org.junit.Test;

import static org.csfundamental.database.storage.DiskSpaceManagerImpl.DATA_PAGES_PER_HEADER;
import static org.csfundamental.database.storage.DiskSpaceManager.PAGE_SIZE;

public class BitsTest {
    @Test
    public void testSetGetCount(){
        byte[] header = new byte[PAGE_SIZE];
        int setBitCount = 0;
        for (int bit = 0; bit < DATA_PAGES_PER_HEADER; bit++){
            Bits.setBit(header, bit, Bits.Bit.ONE);
            int actual = Bits.countBits(header);
            Assert.assertTrue(Bits.getBit(header, bit) == Bits.Bit.ONE);
            Assert.assertEquals(++setBitCount, actual);
        }
    }
}