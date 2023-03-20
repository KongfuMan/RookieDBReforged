package org.csfundamental.database.storage;

import java.io.IOException;

public interface IDiskSpaceManager {
    int PAGE_SIZE = 4096;
    long INVALID_PAGE_NUM = -1L;
    long MAX_PAGE_COUNT = 10000000000L;

    /**
     * Allocate a new partition with id increase by one from current max partition id
     * */
    int allocPart() throws Exception;
    int allocPart(int partNum) throws Exception;

    void freePart(int partNum) throws Exception;

    /**
     * Allocate a new page within a specific partition.
     * @param  partNum: partition number.
     * @return virtual page number.
     * */
    long allocPage(int partNum) throws Exception;

    /**
     * Allocates a new page with a specific page number.
     * @param page: virtual page number of new page
     * @return virtual page number of new page
     */
    long allocPage(long page) throws IOException;

    void freePage(long page) throws Exception;

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
