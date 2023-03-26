package org.csfundamental.database.storage;

import java.io.Closeable;
import java.io.IOException;

public interface DiskSpaceManager extends Closeable {
    int PAGE_SIZE = 4096;
    long INVALID_PAGE_NUM = -1L;
    long MAX_PAGE_COUNT = 10000000000L;

    /**
     * Allocate a new partition with number incremented by one from current max partition number
     *
     * @return id of created partition
     **/
    int allocPart();

    int allocPart(int partNum);

    /**
     * Free a partition with specified partition number.
     * The partition object model is reset in memory and the backing file is deleted.
     **/
    void freePart(int partNum);

    /**
     * Allocate a new page within a specific partition.
     * @param  partNum: partition number.
     * @return virtual page number.
     * */
    long allocPage(int partNum);

    /**
     * Allocates a new page with a specific page number.
     * @param page: virtual page number of new page
     * @return virtual page number of new page
     */
    long allocPage(long page);

    void freePage(long page);

    /**
     * Read a whole page into the byte array in memory
     * @param page: virtual page number.
     * @param data: byte array to save page bytes
     * */
    void readPage(long page, byte[] data);

    /**
     * Write the data byte array into specified page on disk
     * @param page: virtual page number
     * @param data: byte array from which data is saved to page on disk
     * */
    void writePage(long page, byte[] data);

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
