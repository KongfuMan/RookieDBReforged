package org.csfundamental.database.buffer;

/**
 * Cache's critical section is protected by caller's lock
 *
 * */
public interface CacheStrategy {
    /**
     * Get the frame from the cache by virtual page number
     *
     * @param page : virtual page number
     * @return buffer frame if exists; null otherwise.
     */
    BufferFrame get(long page);

    /**
     * Choose a buffer frame which has not been pinned to evict.
     *
     * @return the evicted buffer frame if any.
     */
    BufferFrame put(long page, BufferFrame frame);

    /**
     * Evict the oldest unpinned frame.
     * */
    BufferFrame evict();

    void remove(long page);

    Iterable<BufferFrame> getAllPageFrames();
}
