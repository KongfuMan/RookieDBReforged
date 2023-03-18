package org.csfundamental.database.storage;

public interface IDiskManager {
    int PAGE_SIZE = 4096;
    long INVALID_PAGE_NUM = -1L;
    long MAX_PAGE_COUNT = 10000000000L;

    int allocPart();

    void freePart(int partNum);

    long allocPage();

    int freePage(long page);

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
