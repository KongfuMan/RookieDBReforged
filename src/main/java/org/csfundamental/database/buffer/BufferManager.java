package org.csfundamental.database.buffer;

import org.csfundamental.database.storage.DiskSpaceManager;
import org.csfundamental.database.storage.PageException;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

public class BufferManager implements Closeable {
    // Reserve 36 bytes on each page to book keep info for recovery
    // (used to store the pageLSN, and to ensure that a redo-only/undo-only log record can
    // fit on one page).
    public static final int RESERVED_SPACE = 36;
    public static final int EFFECTIVE_PAGE_SIZE = DiskSpaceManager.PAGE_SIZE - RESERVED_SPACE;

    private final DiskSpaceManager dsm;
    private final CacheStrategy cacheStrategy;
    private final ReentrantLock managerLock;
    private long numIO;

    public BufferManager(DiskSpaceManager dsm, int capacity){
        this.dsm = dsm;
        this.cacheStrategy = new LRUCacheStrategy(capacity);
        this.managerLock = new ReentrantLock();
        this.numIO = 0;
    }

    /**
     * Fetches a buffer frame with data for the specified page. Reuses existing
     * buffer frame if page already loaded in memory. Pins the buffer frame.
     * Cannot be used outside the package.
     *
     * @param page The virtual page number
     * @return buffer frame with specified page loaded
     */
    BufferFrame fetchPageFrame(long page) {
        Frame newFrame;
        Frame evictedFrame;
        managerLock.lock();
        try{
            if (!dsm.pageAllocated(page)){
                throw new PageException("Page not allocated");
            }
            newFrame = (Frame)cacheStrategy.get(page);
            if (newFrame != null){
                return newFrame;
            }
            byte[] data = new byte[DiskSpaceManager.PAGE_SIZE];
            newFrame = new Frame(data, page);
            evictedFrame = (Frame)cacheStrategy.put(page,newFrame);
            if (evictedFrame == null){
                return newFrame;
            }
            evictedFrame.frameLock.lock();
        }finally {
            managerLock.unlock();
        }

        try{
            evictedFrame.invalidate();
        }finally {
            evictedFrame.frameLock.unlock();
        }

        newFrame.frameLock.lock();
        try{
            newFrame.pin();
            // read data into buffer frame from disk;
            dsm.readPage(page, newFrame.content);
            this.incrementNumIO();
            return newFrame;
        }finally {
            newFrame.unpin();
            newFrame.frameLock.unlock();
        }
    }

    /**
     * Fetches the specified page, with a loaded and pinned buffer frame.
     *
     * @param page virtual page number
     * @return specified page
     */
    public Page fetchPage(long page) {
        return this.frameToPage(this.fetchPageFrame(page));
    }

    /**
     * Fetches a buffer frame for a new page. Pins the buffer frame.
     * Cannot be used outside the package.
     *
     * @param partNum partition number for new page
     * @return buffer frame for the new page
     */
    BufferFrame fetchNewPageFrame(int partNum) {
        long page = dsm.allocPage(partNum);
        return fetchPageFrame(page);
    }

    Page fetchNewPage(int partNum) {
        long page = dsm.allocPage(partNum);
        return fetchPage(page);
    }

    /**
     * Frees a page - evicts the page from cache, and tells the disk space manager
     * that the page is no longer needed. Page must be pinned before this call,
     * and cannot be used after this call (aside from unpinning).
     *
     * @param page page to free
     */
    public void freePage(Page page) {
        managerLock.lock();
        long pageNum = page.getPageNum();
        BufferFrame frame = cacheStrategy.get(pageNum);
        frame.flush();
        cacheStrategy.remove(pageNum);
        managerLock.unlock();
    }

    public void freePart(int partNum) {
        managerLock.lock();
        try{
            //TODO: evict all pages in this partition

            dsm.freePart(partNum);
        }finally {
            managerLock.unlock();
        }
    }

    /**
     *
     * */
    public void evict(long page) {
        managerLock.lock();
        BufferFrame frame = cacheStrategy.get(page);
        if (frame != null && !frame.isPinned()){
            cacheStrategy.remove(page);
            frame.invalidate();
        }
        managerLock.unlock();
    }

    public void evictAll() {
        for(BufferFrame frame  : cacheStrategy.getAllPageFrames()){
            evict(frame.getPageNum());
        }
    }

    public void iteratePagesByPageNumber(BiConsumer<Long, Boolean> process) {

    }

    private void incrementNumIO(){
        managerLock.lock();
        ++numIO;
        managerLock.unlock();
    }

    public long getNumIOs() {
        long res = 0;
        managerLock.lock();
        res = numIO;
        managerLock.unlock();
        return res;
    }

    public void close() throws IOException {

    }

    /**
     * Wraps a frame in a page object.
     * @param frame frame for the page
     * @return page object
     */
    private Page frameToPage(BufferFrame frame) {
        return new Page(frame);
    }

    class Frame extends BufferFrame {
        private final byte[] content;
        private final long page;
        final ReentrantLock frameLock;
        private boolean dirty;

        /**
         * Mark if the buffer frame is evicted.
         * */
        private boolean isValid;

        public Frame(byte[] content, long page){
            if (Objects.requireNonNull(content).length != DiskSpaceManager.PAGE_SIZE){
                throw new IllegalArgumentException("Illegal input byte array");
            }
            this.content = content;
            this.page = page;
            this.dirty = false;
            this.frameLock = new ReentrantLock();
            this.isValid = true;
        }

        @Override
        long getPageNum() {
            return page;
        }

        @Override
        void flush() {
            frameLock.lock();
            pin();
            try {
                BufferManager.this.dsm.writePage(this.page, this.content);
            }finally {
                unpin();
                frameLock.unlock();
            }
        }

        @Override
        void readBytes(short position, short len, byte[] buf) {
            frameLock.lock();
            pin();
            System.arraycopy(content, position, buf, 0, len);
            unpin();
            frameLock.unlock();
        }

        @Override
        void writeBytes(short position, short len, byte[] buf) {
            frameLock.lock();
            pin();
            // TODO: transaction atomicity and durability
            System.arraycopy(buf, 0, content, position, len);
            dirty = true;
            unpin();
            frameLock.unlock();
        }

        @Override
        void invalidate(){
            this.isValid = false;
        }

        @Override
        boolean isValid(){
            return isValid;
        }

        @Override
        void setPageLSN(long pageLSN) {
        }

        @Override
        long getPageLSN() {
            return 0;
        }
    }
}

