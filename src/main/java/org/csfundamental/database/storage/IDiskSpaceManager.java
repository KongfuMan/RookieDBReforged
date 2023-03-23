package org.csfundamental.database.storage;

import java.io.IOException;

public interface IDiskSpaceManager {
    int PAGE_SIZE = 4096;
    long INVALID_PAGE_NUM = -1L;
    long MAX_PAGE_COUNT = 10000000000L;

    /**
     * Allocate a new partition with number incremented by one from current max partition number
     *
     * @return id of created partition
     * */
    int allocPart() throws IOException;
    int allocPart(int partNum) throws IOException;
    void freePart(int partNum) throws IOException;

    /**
     * Allocate a new page within a specific partition.
     * @param  partNum: partition number.
     * @return virtual page number.
     * */
    long allocPage(int partNum) throws IOException;

    /**
     * Allocates a new page with a specific page number.
     * @param page: virtual page number of new page
     * @return virtual page number of new page
     */
    long allocPage(long page) throws IOException;

    void freePage(long page) throws IOException;

    /**
     * Read a whole page into the byte array in memory
     * @param page: virtual page number.
     * @param data: byte array to save page bytes
     * */
    void readPage(long page, byte[] data) throws IOException;

    /**
     * Write the data byte array into specified page on disk
     * @param page: virtual page number
     * @param data: byte array from which data is saved to page on disk
     * */
    void writePage(long page, byte[] data) throws IOException;

    int getCurrentPartNum();
    static int getPartNum(long page){
        return (int)(page / MAX_PAGE_COUNT);
    }

    /**
     * Convert virtual page number to logic page number scoped in single partition.
     * */
    static int getPageNum(long page){
        return (int)(page % MAX_PAGE_COUNT);
    }

    static long getVirtualPageNum(int partNum, int pageNum){
        return partNum * MAX_PAGE_COUNT + pageNum;
    }
}
