package org.csfundamental.database.buffer;

import org.csfundamental.database.storage.DiskSpaceManager;
import org.csfundamental.database.storage.PageException;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

/**
 * Buffer manager follows the STEAL & NO-FORCE policy of ARIES recovery protocol for better performance.
 * STEAL: the frame can be swapped out before the trx commit.
 * NO-FORCE: buffer frame updated by the trx will not be flushed immediately after
 *           trx commit.
 * */
public class BufferManager implements AutoCloseable {
    // Reserve 36 bytes on each page to book keep info for recovery
    // (used to store the pageLSN, and to ensure that a redo-only/undo-only log record can
    // fit on one page).
    public static final int RESERVED_SPACE = 36;
    public static final int EFFECTIVE_PAGE_SIZE = DiskSpaceManager.PAGE_SIZE - RESERVED_SPACE;

    private final DiskSpaceManager diskSpaceManager;
    private final CacheStrategy cacheStrategy;
    private final ReentrantLock managerLock;
    private long numIO;

    private final Set<BufferFrame> inValidFrames;

    public BufferManager(DiskSpaceManager diskSpaceManager, int capacity){
        this.diskSpaceManager = diskSpaceManager;
        this.cacheStrategy = new LRUCacheStrategy(capacity);
        this.managerLock = new ReentrantLock();
        this.numIO = 0;
        this.inValidFrames = new HashSet<>();
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
        return fetchPageFrame(page, false);
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
        long page = diskSpaceManager.allocPage(partNum);
        return fetchPageFrame(page, true);
    }

    public Page fetchNewPage(int partNum) {
        long page = diskSpaceManager.allocPage(partNum);
        return this.frameToPage(this.fetchPageFrame(page, true));
    }

    /**
     * Fetches a buffer frame for the specified page.
     * @param page The virtual page number
     * @param newAllocated indicating whether the page is newly allocated one, s.t. no load action needed.
     * @return buffer frame which must be pinned to prevent being swapped out.
     */
    private BufferFrame fetchPageFrame(long page, boolean newAllocated) {
        Frame newFrame;
        Frame evictedFrame;
        managerLock.lock();
        try{
            if (!diskSpaceManager.pageAllocated(page)){
                throw new PageException("Page not allocated");
            }
            newFrame = (Frame)cacheStrategy.get(page);
            if (newFrame != null){
                // cache hit, then just return the frame
                newFrame.pin();
                return newFrame;
            }
            // cache miss, either because the frame was previously swapped out or this is a newly allocated page
            byte[] data = new byte[DiskSpaceManager.PAGE_SIZE];
            newFrame = new Frame(data, page);
            evictedFrame = (Frame)cacheStrategy.put(page,newFrame);
        }finally {
            managerLock.unlock();
        }

        if (evictedFrame != null){
            evictedFrame.frameLock.lock();
            try{
                evictedFrame.invalidate();
                inValidFrames.add(evictedFrame);
            }finally {
                evictedFrame.frameLock.unlock();
            }
        }

        newFrame.frameLock.lock();
        try{
            newFrame.pin();
            if (newAllocated){
                // newly allocated page on-disk, so no load action needed.
                return newFrame;
            }
            // read data into buffer frame from disk;
            diskSpaceManager.readPage(page, newFrame.content);
            this.incrementNumIO();
            return newFrame;
        }finally {
            newFrame.frameLock.unlock();
        }
    }

    /**
     * Frees a page - evicts the page from cache, and tells the disk space manager
     * that the page is no longer needed. Page must be pinned before this call,
     * and cannot be used after this call (aside from unpinning).
     *
     * @param page page to free
     */
    public void freePage(Page page) {
        freePage(page.getPageNum());
    }

    private void freePage(long pageNum){
        managerLock.lock();
        try{
            BufferFrame frame = cacheStrategy.get(pageNum);
            frame.flush();
            frame.invalidate(); //should not allow any action after deallocate
            cacheStrategy.remove(pageNum);
            diskSpaceManager.freePage(pageNum);
        }finally {
            managerLock.unlock();
        }

    }

    public void freePart(int partNum) {
        managerLock.lock();
        try{
            // TODO: evict all pages in this partition
            Iterator<BufferFrame> itr = cacheStrategy.getAllPageFrames().iterator();
            while(itr.hasNext()){
                BufferFrame frame = itr.next();
                if (DiskSpaceManager.getPartNum(frame.getPageNum()) == partNum){
                    this.freePage(frame.getPageNum());
                }
            }
            diskSpaceManager.freePart(partNum);
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

    /**
     * Private method. Called should guarantee thread-safety
     * */
    private void incrementNumIO(){
        ++numIO;
    }

    long getNumIOs() {
        return numIO;
    }

    public void close() {

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
        private byte[] content;
        private final long page;
        final ReentrantLock frameLock;
        private boolean dirty;
        private boolean logPage;

        /**
         * Mark if the buffer frame is going to be reclaimed.
         * A reclaim worker will later reclaim invalid frame whose pinCount == 0.
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
            this.logPage = false;
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
                if (!this.dirty){
                    return;
                }
                BufferManager.this.diskSpaceManager.writePage(this.page, this.content);
                BufferManager.this.incrementNumIO();
                this.dirty = false;
            }finally {
                unpin();
                frameLock.unlock();
            }
        }

        @Override
        void readBytes(short position, short len, byte[] buf) {
            frameLock.lock();
            try{
                if (!this.isValid()){
                    throw new IllegalStateException("Reading from invalid buffer frame");
                }
                pin();
                System.arraycopy(content, position + dataOffset(), buf, 0, len);
            }finally {
                unpin();
                frameLock.unlock();
            }
        }

        @Override
        void writeBytes(short position, short len, byte[] buf) {
            frameLock.lock();
            try{
                if (!this.isValid()){
                    throw new IllegalStateException("Writing to invalid buffer frame");
                }
                pin();
                // TODO: transaction atomicity and durability

                System.arraycopy(buf, 0, content, position + dataOffset(), len);
                this.dirty = true;
            }finally {
                unpin();
                frameLock.unlock();
            }
        }

        private short dataOffset() {
            if (logPage) {
                return 0;
            } else {
                return BufferManager.RESERVED_SPACE;
            }
        }

        /**
         * The frame is marked invalid if it is going to be reclaimed from memory.
         * As a result, no action is allowed except for unpin.
         * */
        @Override
        void invalidate(){
            if (this.isValid()){
                // supposed to be a worker thread to flush. Do it now to make test easier.
                this.flush();
            }
            this.isValid = false;
            // make it GCed
            this.content = null;
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

