package org.csfundamental.database.storage;

import java.util.Map;

public class DiskManagerImpl implements IDiskManager{
    private Map<Integer, Partition> partitionMap;

    /**
     * PAGE_SIZE / 2 byte
     * */
    public static final int HEADER_PAGES_PER_MASTER = PAGE_SIZE / 2;

    /**
     * PAGE_SIZE / 1 bit
     * */
    public static final int DATA_PAGES_PER_HEADER = PAGE_SIZE * 8;

    @Override
    public int allocPart() {
        return 0;
    }

    @Override
    public void freePart(int partNum) {

    }

    @Override
    public long allocPage() {
        return 0;
    }

    @Override
    public int freePage(long page) {
        return 0;
    }

    @Override
    public void readPage(long page, byte[] data) {

    }

    @Override
    public void writePage(long page, byte[] data) {

    }
}
