package org.csfundamental.database.buffer;

/**
 * Page is exposed to upper level application to get a PageBuffer for read/write on-disk page.
 * One frame corresponds to one-page object which can return multiple page buffers.
 * Multiple page buffers are associated with same page
 * */
public class Page {
    private BufferFrame frame;

    // TODO: page lock???

    public Page(BufferFrame frame){
        this.frame = frame;
    }

    public PageBuffer getPageBuffer(){
        return new PageBuffer(this);
    }

    public long getPageNum(){
        return frame.getPageNum();
    }

    BufferFrame getFrame(){
        return frame;
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

    private class PageBuffer {
        Page page;
        int offset;

        public PageBuffer(Page page){
            this.page = page;
        }
    }
}
