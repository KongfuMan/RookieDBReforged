package org.csfundamental.database.buffer;

import org.csfundamental.database.common.AbstractBuffer;
import org.csfundamental.database.common.Buffer;

/**
 * Page is exposed to upper level application to get a PageBuffer for read/write on-disk page.
 * One frame corresponds to one-page object which can return multiple page buffers.
 * Multiple page buffers are associated with same page
 * */
public class Page {
    private BufferFrame frame;

    // TODO: context lock???
    private Object lockContext;

    public Page(BufferFrame frame){
        this.frame = frame;
    }

    protected Page(Page page) {
        this(page.frame);
    }

    public Buffer getPageBuffer(){
        return new PageBuffer();
    }

    /**
     * Reads num bytes from offset position into buf.
     *
     * @param position the offset in the page to read from
     * @param num the number of bytes to read
     * @param buf the buffer to put the bytes into
     */
    private void readBytes(int position, int num, byte[] buf) {

    }

    /**
     * Read all the bytes in file.
     *
     * @return a new byte array with all the bytes in the file
     */
    private byte[] readBytes() {
        return null;
    }

    /**
     * Write num bytes from buf at offset position.
     *
     * @param position the offest in the file to write to
     * @param num the number of bytes to write
     * @param buf the source for the write
     */
    private void writeBytes(int position, int num, byte[] buf) {

    }

    /**
     * Write all the bytes in file.
     */
    private void writeBytes(byte[] data) {

    }

    public long getPageNum(){
        return frame.getPageNum();
    }


    /**
     * Implementation of Buffer for the page data. All reads/writes ultimately wrap around
     * Page#readBytes and Page#writeBytes, which delegates work to the buffer manager.
     */
    private class PageBuffer extends AbstractBuffer {
        private int offset;

        private PageBuffer() {
            this(0, 0);
        }

        private PageBuffer(int offset, int position) {
            super(position);
            this.offset = offset;
        }

        /**
         * All read operations through the Page object must run through this method.
         *
         * @param dst destination byte buffer
         * @param offset offset into page to start reading
         * @param length number of bytes to read
         * @return this
         */
        @Override
        public Buffer get(byte[] dst, int offset, int length) {
            // TODO(proj4_part2): Update the following line
            Page.this.readBytes(this.offset + offset, length, dst);
            return this;
        }

        /**
         * All write operations through the Page object must run through this method.
         *
         * @param src source byte buffer (to copy to the page)
         * @param offset offset into page to start writing
         * @param length number of bytes to write
         * @return this
         */
        @Override
        public Buffer put(byte[] src, int offset, int length) {
            // TODO(proj4_part2): Update the following line
            Page.this.writeBytes(this.offset + offset, length, src);
            return this;
        }

        /**
         * Create a new PageBuffer starting at the current offset.
         * @return new PageBuffer starting at the current offset
         */
        @Override
        public Buffer slice() {
            return new PageBuffer(offset + position(), 0);
        }

        /**
         * Create a duplicate PageBuffer object
         * @return PageBuffer that is functionally identical to this one
         */
        @Override
        public Buffer duplicate() {
            return new PageBuffer(offset, position());
        }
    }
}