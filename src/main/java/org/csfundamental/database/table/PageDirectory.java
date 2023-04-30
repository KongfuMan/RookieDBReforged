package org.csfundamental.database.table;

import org.csfundamental.database.buffer.BufferFrame;
import org.csfundamental.database.buffer.BufferManager;
import org.csfundamental.database.buffer.Page;
import org.csfundamental.database.common.Buffer;
import org.csfundamental.database.common.ByteBuffer;
import org.csfundamental.database.common.iterator.BacktrackingIterable;
import org.csfundamental.database.common.iterator.BacktrackingIterator;
import org.csfundamental.database.common.iterator.ConcatBacktrackingIterator;
import org.csfundamental.database.common.iterator.IndexBacktrackingIterator;
import org.csfundamental.database.storage.DiskSpaceManager;
import org.csfundamental.database.storage.PageException;

import java.util.Random;

/**
 * An implementation of a heap file, using a page directory.
 * Assumes data pages are packed (but record lengths do not need to be fixed-length).
**/
public class PageDirectory implements BacktrackingIterable<Page> {
    public static final short HEADER_HEADER_SIZE = 13;

    // number of data page entries in a header page
    public static final short HEADER_ENTRY_COUNT = (BufferManager.EFFECTIVE_PAGE_SIZE -
            HEADER_HEADER_SIZE) / DataPageEntry.SIZE;

    // size of the header in data pages
    public static final short DATA_HEADER_SIZE = 10;

    // effective page size
    public static final short EFFECTIVE_PAGE_SIZE = BufferManager.EFFECTIVE_PAGE_SIZE -
            DATA_HEADER_SIZE;

    private BufferManager bufferManager;
    private HeaderPage firstHeader;

    /**
     * Single partition that saves all the header paged
     **/
    private int partNum;
    private int pageDirectoryId;

    /**
     * Creates a new heap file, or loads existing file if one already
     * exists at partNum.
     * @param bufferManager buffer manager
     * @param partNum partition to allocate new header pages in (can be different partition
     *                from data pages)
     * @param pageNum first header page of page directory
//     * @param emptyPageMetadataSize size of metadata on an empty page
//     * @param lockContext lock context of this heap file
     */
    public PageDirectory(BufferManager bufferManager, int partNum, long pageNum) {
        this.bufferManager = bufferManager;
        this.partNum = partNum;
        this.firstHeader = new HeaderPage(pageNum, 0, true);
    }

    public short getEffectivePageSize() {
        return EFFECTIVE_PAGE_SIZE;
    }

    /**
     * Get the allocated data page by virtual page number.
     * **/
    public Page fetchPage(long pageNum){
        // get page by page number
        Page page = bufferManager.fetchPage(pageNum);
        return new DataPage(pageDirectoryId, page);
    }

    /**
     * Request a data page that has more space than required.
     * This page could either be one that has already been allocated and has enough free space,
     * or it could be a new page created within this page directory
     *
     * @param requiredSpace space required in bytes.
     * **/
    public Page fetchPageWithSpace(short requiredSpace){
        if (requiredSpace <= 0) {
            throw new IllegalArgumentException("cannot request non positive amount of space");
        }
        if (requiredSpace > EFFECTIVE_PAGE_SIZE) {
            throw new IllegalArgumentException("requesting page with more space than the size of the page");
        }

        Page page = this.firstHeader.fetchPageWithSpace(requiredSpace);
//        LockContext pageContext = lockContext.childContext(page.getPageNum());
        // TODO(proj4_part2): Update the following line
//        LockUtil.ensureSufficientLockHeld(pageContext, LockType.NL);

        return new DataPage(pageDirectoryId, page);
    }

    /**
     * Update the free space of header page managing the data page.
     *
     * @param page data page to update the space
     * @param  newFreeSpace the new space size in byte.
     * **/
    public void updateFreeSpace(Page page, short newFreeSpace) {
        if (newFreeSpace <= 0 || newFreeSpace > EFFECTIVE_PAGE_SIZE) {
            throw new IllegalArgumentException("bad size for data page free space");
        }
        int headerIndex;
        short entryIndex;
        page.pin();
        try{
            // find the header page managing the data page
            Buffer pageBuffer = page.getBuffer();
            pageBuffer.position(4); // skip page directory id
            headerIndex = pageBuffer.getInt();
            entryIndex = pageBuffer.getShort();
        } finally {
            page.unpin();
        }

        HeaderPage header = firstHeader;
        for(int i = 0; i < headerIndex; i++){
            header = header.next;
        }

        header.updateSpace(page, entryIndex, newFreeSpace);
    }

    @Override
    public BacktrackingIterator<Page> iterator() {
        return new ConcatBacktrackingIterator<>(new HeaderPageIterator());
    }

    public int getNumDataPages(){
        int numDataPages = 0;
        HeaderPage headerPage = firstHeader;
        while(headerPage != null){
            numDataPages += headerPage.numDataPages;
            headerPage = headerPage.next;
        }
        return numDataPages;
    }
    public int getPartNum() {
        return partNum;
    }

    /**
     * Data pages contain a small header containing:
     * - 4-byte page directory id
     * - 4-byte index of which header page manages it
     * - 2-byte offset indicating which slot in the header page its data page entry resides
     */
    private class DataPage extends Page {
        public DataPage(BufferFrame frame) {
            super(frame);
        }

        /**
         * Create a data page by the general page.
         * **/
        public DataPage(int pageDirectoryId, Page page){
            super(page);

            Buffer buffer = super.getBuffer();
            if (buffer.getInt() != pageDirectoryId) {
                page.unpin();
                throw new PageException("data page directory id does not match");
            }
        }
    }

    private static class DataPageEntry {
        public static final int SIZE = 10;
        long pagNum; // 8 bytes
        short freeSpace; // 2 bytes (unsigned short)

        public DataPageEntry() {
            this(DiskSpaceManager.INVALID_PAGE_NUM, (short)-1);
        }

        public DataPageEntry(long pagNum, short freeSpace) {
            this.pagNum = pagNum;
            this.freeSpace = freeSpace;
        }

        public boolean isValid(){
            return this.pagNum != DiskSpaceManager.INVALID_PAGE_NUM;
        }

        /**
         * Deserialize the byte array to create a data page entry
         * **/
        private static DataPageEntry fromBytes(Buffer pageBuffer){
            return new DataPageEntry(pageBuffer.getLong(), pageBuffer.getShort());
        }

        /**
         * Serialize the data page entry to the bytes.
         * **/
        private void toBytes(Buffer pageBuffer){
            pageBuffer.putLong(this.pagNum).putShort((short)freeSpace);
        }
    }

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
    private class HeaderPage implements BacktrackingIterable<Page> {
        private static final byte HEADER_ALLOCATED = (byte)1;
        private static final int NEXT_HEADER_POSITION = 5;
        private HeaderPage next;
        private Page page;
        private short numDataPages;
        private int headerOffset;

        /**
         * Constructor: load a header page by its virtual page number from disk.
         * @param pageNum virtual page number of header page of this page directory.
         * @param headerOffset the page offset of this header page from the first header page
         *                     for this page directory.
         * @param firstHeader is first header page.
         * */
        public HeaderPage(long pageNum, int headerOffset, boolean firstHeader){
            // fetch header page by page number. return page are pinned.
            this.page = bufferManager.fetchPage(pageNum);
            this.numDataPages = 0;
            long nextPageNum;
            try {
                Buffer pageBuffer = page.getBuffer();
                if (pageBuffer.get() == HEADER_ALLOCATED){
                    //load allocated header page
                    int pageDirectoryId = pageBuffer.getInt();
                    if (firstHeader){
                        PageDirectory.this.pageDirectoryId = pageDirectoryId;
                    }else if (PageDirectory.this.pageDirectoryId != pageDirectoryId){
                        throw new PageException("header page does not belongs to this page directory");
                    }

                    nextPageNum = pageBuffer.getLong();
                    for (int i = 0; i < HEADER_ENTRY_COUNT; i++){
                        DataPageEntry dataPageEntry = DataPageEntry.fromBytes(pageBuffer);
                        if (dataPageEntry.isValid()){
                            numDataPages++;
                        }
                    }
                }else{
                    // header page not allocated yet. Initialize
                    byte[] buf = new byte[BufferManager.EFFECTIVE_PAGE_SIZE];
                    Buffer tempHeaderBuf = ByteBuffer.wrap(buf);
                    // invalid page, initialize empty header page
                    if (firstHeader){
                        pageDirectoryId = new Random().nextInt();
                    }
                    nextPageNum = DiskSpaceManager.INVALID_PAGE_NUM;
                    tempHeaderBuf.position(0).put(HEADER_ALLOCATED)
                                 .putInt(pageDirectoryId)
                                 .putLong(DiskSpaceManager.INVALID_PAGE_NUM);
                    DataPageEntry invalidDataPageEntry = new DataPageEntry();
                    for (int i = 0; i < HEADER_ENTRY_COUNT; i++){
                        invalidDataPageEntry.toBytes(tempHeaderBuf);
                    }
                    pageBuffer.put(buf, 0, buf.length);
                }
            }finally {
                this.page.unpin();
            }
            this.headerOffset = headerOffset;
            if (nextPageNum != DiskSpaceManager.INVALID_PAGE_NUM){
                this.next = new HeaderPage(nextPageNum, headerOffset + 1, false);
            }else {
                this.next = null;
            }
        }

        /**
         * Load a data page with the free space no less than required space.
         * For all the data pages managed by the header page, find the target page following
         * the steps below:
         * 1. if there is an allocated page that has enough space, then return that page.
         * 2. else return the first free page if any.
         * 3. If no free data page included in this header, check data pages managed by the next header page.
         * **/
        private Page fetchPageWithSpace(short requiredSpace){
            this.page.pin();
            try{
                // buffer associated with the header page
                Buffer pageBuffer = this.page.getBuffer();
                pageBuffer.position(HEADER_HEADER_SIZE);

                short unusedSlot = -1;
                for(int i = 0; i < HEADER_ENTRY_COUNT; i++){
                    DataPageEntry dataPageEntry = DataPageEntry.fromBytes(pageBuffer);
                    if (!dataPageEntry.isValid()){
                        if (unusedSlot == -1){
                            unusedSlot = (short)i;
                        }
                        continue;
                    }

                    if (dataPageEntry.freeSpace >= requiredSpace){
                        // use this page: pre-deduct the free space
                        dataPageEntry.freeSpace -= requiredSpace;
                        pageBuffer.position(pageBuffer.position() - DATA_HEADER_SIZE);
                        // overwrite this data page entry
                        dataPageEntry.toBytes(pageBuffer);
                        return bufferManager.fetchPage(dataPageEntry.pagNum);
                    }
                }

                // find first unallocated
                if (unusedSlot != -1){
                    // allocate a new page
                    Page dataPage = bufferManager.fetchNewPage(partNum);
                    DataPageEntry dataPageEntry = new DataPageEntry(dataPage.getPageNum(), (short)(EFFECTIVE_PAGE_SIZE - requiredSpace));
                    pageBuffer.position(HEADER_HEADER_SIZE + unusedSlot * DATA_HEADER_SIZE);
                    dataPageEntry.toBytes(pageBuffer);

                    dataPage.getBuffer().putInt(pageDirectoryId).putInt(headerOffset).putShort(unusedSlot);
                    ++this.numDataPages;
                    return dataPage;
                }

                // if we have no next header page, make one
                if (this.next == null) {
                    this.addNewHeaderPage();
                }

                // no space on this header page, try next one
                return this.next.fetchPageWithSpace(requiredSpace);
            }finally {
                this.page.unpin();
            }
        }

        /**
         * update free space of data pag entry of the header page managing the data page
         * @param dataPage
         * @param index of the data page entry in the header page
         * @param newFreeSpace
         * **/
        private void updateSpace(Page dataPage, short index, short newFreeSpace){
            page.pin();
            try{
                Buffer headerBuf = page.getBuffer();
                if (newFreeSpace < EFFECTIVE_PAGE_SIZE){
                    headerBuf.position(HEADER_HEADER_SIZE + index * DATA_HEADER_SIZE);
                    DataPageEntry dataPageEntry = DataPageEntry.fromBytes(headerBuf);
                    dataPageEntry.freeSpace = newFreeSpace;
                    headerBuf.position(HEADER_HEADER_SIZE + index * DATA_HEADER_SIZE);
                    dataPageEntry.toBytes(headerBuf);
                }else{
                    //newFreeSpace is larger than the free space of a page, just deallocate the page
                    DataPageEntry dataPageEntry = new DataPageEntry();
                    headerBuf.position(HEADER_HEADER_SIZE + index * DATA_HEADER_SIZE);
                    dataPageEntry.toBytes(headerBuf);
                    bufferManager.freePage(dataPage);
                }
            }finally {
                page.unpin();
            }
        }

        private void addNewHeaderPage() {
            if (next != null){
                this.next.addNewHeaderPage();
                return;
            }
            Page page = bufferManager.fetchNewPage(partNum); //page has pinned
            this.page.pin();
            try{
                next = new HeaderPage(page.getPageNum(), headerOffset + 1 ,false);
                // update next header page num field for current header page
                this.page.getBuffer().position(NEXT_HEADER_POSITION).putLong(page.getPageNum());
            }finally {
                this.page.unpin();
                page.unpin();
            }
        }

        @Override
        public BacktrackingIterator<Page> iterator() {
            return new DataPageIterator();
        }

        /**
         * Iterator over data pages managed by this header page.
         * The data page entry array managed by header page are sparse.
         */
        class DataPageIterator extends IndexBacktrackingIterator<Page> {

            public DataPageIterator(){
                super(HEADER_ENTRY_COUNT);
            }

            @Override
            protected int getNextNonEmpty(int currentIndex) {
                HeaderPage.this.page.pin();
                try{
                    Buffer pageBuffer = HeaderPage.this.page.getBuffer();
                    currentIndex++;
                    pageBuffer.position(HEADER_HEADER_SIZE + currentIndex * DATA_HEADER_SIZE);
                    for (int i = currentIndex; i < HEADER_ENTRY_COUNT; i++){
                        DataPageEntry dataPageEntry = DataPageEntry.fromBytes(pageBuffer);
                        if (dataPageEntry.isValid()){
                            return i;
                        }
                    }
                    return HEADER_HEADER_SIZE;
                }finally {
                    HeaderPage.this.page.unpin();
                }
            }

            /**
             * Get data page
             * **/
            @Override
            protected Page getValue(int index) {
                HeaderPage.this.page.pin();
                try {
                    Buffer pageBuffer = HeaderPage.this.page.getBuffer();
                    pageBuffer.position(HEADER_HEADER_SIZE + index * DATA_HEADER_SIZE);
                    DataPageEntry dataPageEntry = DataPageEntry.fromBytes(pageBuffer);
                    return PageDirectory.this.fetchPage(dataPageEntry.pagNum);
                }finally {
                    HeaderPage.this.page.unpin();
                }
            }
        }
    }

    /**
     * Iterator over the header pages of this page directory.
     * */
    class HeaderPageIterator implements BacktrackingIterator<BacktrackingIterable<Page>> {
        private HeaderPage nextHeaderPage;
        private HeaderPage prevHeaderPage;
        private HeaderPage markedHeaderPage;

        private HeaderPageIterator() {
            this.nextHeaderPage = firstHeader;
            this.prevHeaderPage = null;
            this.markedHeaderPage = null;
        }

        @Override
        public void markPrev() {
            if (this.prevHeaderPage != null){
                markedHeaderPage = this.prevHeaderPage;
            }
        }

        @Override
        public void markNext() {
            markedHeaderPage = this.nextHeaderPage.next;
        }

        @Override
        public void reset() {
            if (this.markedHeaderPage != null){
                this.nextHeaderPage = this.markedHeaderPage;
                this.prevHeaderPage = null;
            }
        }

        @Override
        public boolean hasNext() {
            return nextHeaderPage != null;
        }

        @Override
        public BacktrackingIterable<Page> next() {
            HeaderPage result = this.nextHeaderPage;
            if (prevHeaderPage == null){
                prevHeaderPage = this.nextHeaderPage;
            }else{
                prevHeaderPage = prevHeaderPage.next;
            }
            this.nextHeaderPage = this.nextHeaderPage.next;
            return result;
        }
    }
}
