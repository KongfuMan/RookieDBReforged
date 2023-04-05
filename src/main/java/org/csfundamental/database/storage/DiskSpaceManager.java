package org.csfundamental.database.storage;

import java.io.Closeable;
import java.io.IOException;

public interface DiskSpaceManager extends Closeable {
    int PAGE_SIZE = 4096;
    long INVALID_PAGE_NUM = -1L;
    long MAX_PAGE_COUNT = 10000000000L;

    /**
     * Allocate a new partition with number incremented by one from current maximum partition number
     *
     * @return The ID of the created partition.
     **/
    int allocPart();

    /**
     * Allocates a new partition with a specified partition number.
     *
     * @param partNum The partition number.
     * @return The ID of the created partition.
     **/
    int allocPart(int partNum);

    /**
     * Frees a partition with the specified partition number.
     * The partition object model is reset in memory and the backing file is deleted.
     *
     * @param partNum The partition number.
     **/
    void freePart(int partNum);

    /**
     * Allocate a new page within a specific partition.
     *
     * @param partNum The partition number.
     * @return The virtual page number.
     * */
    long allocPage(int partNum);

    /**
     * Allocates a new page with a specific virtual page number.
     *
     * @param page virtual page number of new page
     * @return virtual page number of new page
     */
    long allocPage(long page);

    /**
     * Frees a page with the specific virtual page number.
     *
     * @param page The virtual page number.
     */
    void freePage(long page);

    /**
     * Read a whole page into the byte array in memory.
     *
     * @param page virtual page number.
     * @param data byte array to save page bytes.
     * */
    void readPage(long page, byte[] data);

    /**
     * Write the data byte array into the specified page on disk
     *
     * @param page The virtual page number
     * @param data The byte array from which data is saved into page on disk
     * */
    void writePage(long page, byte[] data);

    /**
     * Checks if a page is allocated
     *
     * @param page number of page to check
     * @return true if the page is allocated, false otherwise
     */
    boolean pageAllocated(long page);

    int getCurrentPartNum();
    static int getPartNum(long page){
        return (int)(page / MAX_PAGE_COUNT);
    }

    /**
     * Converts a virtual page number to logic page number scoped in single partition.
     * */
    static int getPageNum(long page){
        return (int)(page % MAX_PAGE_COUNT);
    }

    static long getVirtualPageNum(int partNum, int pageNum){
        return partNum * MAX_PAGE_COUNT + pageNum;
    }
}
