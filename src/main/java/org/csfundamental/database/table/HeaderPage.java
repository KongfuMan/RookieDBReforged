package org.csfundamental.database.table;

import org.csfundamental.database.buffer.BufferManager;

/**
 * Header page of page directory. Layout:
 * 1 byte: Is header allocated
 * 4 byte: ID of page directory this header page belongs to
 * 8 byte: Page number of next header page
 * 10 byte:
 *      8 byte: Page number of data page of this page directory
 *      2 byte: Free space in data page in byte
 * repeat 10 byte.
 * */
public class HeaderPage {
    private static final short HEADER_HEADER_SIZE = 13;

    // number of data page entries in a header page
    private static final short HEADER_ENTRY_COUNT = (BufferManager.EFFECTIVE_PAGE_SIZE -
            HEADER_HEADER_SIZE) / DataPageEntry.SIZE;

    // size of the header in data pages
    private static final short DATA_HEADER_SIZE = 10;

    // effective page size
    public static final short EFFECTIVE_PAGE_SIZE = BufferManager.EFFECTIVE_PAGE_SIZE -
            DATA_HEADER_SIZE;
    private class DataPageEntry {
        public static final int SIZE = 10;
        long pagNum;
        int freeSpace;
    }

    private HeaderPage next;
    byte isAllocated;
    int pageDirectoryId;
    long nextPageNum;
//    DataPageEntry[] dataMeta;

    public HeaderPage(int pageDirectoryId){
        isAllocated = 0;
        this.pageDirectoryId = pageDirectoryId;
        this.nextPageNum = -1;
//        dataMeta = new DataPageEntry[];
    }
}
