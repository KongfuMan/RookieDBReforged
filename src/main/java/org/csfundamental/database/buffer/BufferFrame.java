package org.csfundamental.database.buffer;

/**
 * Buffer frame represents a loaded on-disk page in memory.
 */
public abstract class BufferFrame {
    Object tag = null;
    private int pinCount = 0;

    /**
     * Pin buffer frame. Called before read/write data to this frame
     * It cannot be evicted while pinned.
     * This method is paired with unpin.
     */
    protected void pin() {
        ++pinCount;
    }

    /**
     * Unpin buffer frame. Called after read/write the frame
     */
    protected void unpin() {
        if (!isPinned()) {
            throw new IllegalStateException("Cannot unpin an unpinned frame.");
        }
        --pinCount;
    }

    /**
     * @return whether this frame is pinned
     */
    boolean isPinned() {
        return pinCount > 0;
    }

    /**
     * @return virtual page number of this frame
     */
    abstract long getPageNum();

    /**
     * Flushes this buffer frame to disk, but does not unload it.
     */
    abstract void flush();

    /**
     * Read from the buffer frame.
     * @param position position in buffer frame to start reading
     * @param len number of bytes to read
     * @param buf output buffer
     */
    abstract void readBytes(short position, short len, byte[] buf);

    /**
     * Write to the buffer frame following "NO Force" policy.
     * Writing without actual flushing the data to disk, and mark frame as dirtied.
     * @param position position in buffer frame to start writing
     * @param len number of bytes to write
     * @param buf input buffer
     */
    abstract void writeBytes(short position, short len, byte[] buf);

    /**
     * @return amount of space available to user of the frame
     */
    short getEffectivePageSize() {
        return BufferManager.EFFECTIVE_PAGE_SIZE;
    }

    /**
     * Called when the buffer frame is to be evicted. The frame will not be flushed into disk immediately.
     * Instead, a background thread will later to reclaim the evicted frame and flush the data.
     * */
    abstract void invalidate();

    abstract boolean isValid();

    /**
     * @param pageLSN new pageLSN of the page loaded in this frame
     */
    abstract void setPageLSN(long pageLSN);

    /**
     * @return pageLSN of the page loaded in this frame
     */
    abstract long getPageLSN();
}
