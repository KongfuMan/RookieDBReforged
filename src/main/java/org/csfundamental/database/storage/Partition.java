package org.csfundamental.database.storage;

import org.csfundamental.database.common.Bits;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import static org.csfundamental.database.storage.DiskSpaceManagerImpl.HEADER_PAGES_PER_MASTER;
import static org.csfundamental.database.storage.DiskSpaceManagerImpl.DATA_PAGES_PER_HEADER;
import static org.csfundamental.database.storage.DiskSpaceManager.PAGE_SIZE;

/**
 * One partition is backed by on OS file.
 * The methods are not thread-safe. It is the caller's responsibility to use partLock to protect critical section.
 * */
public class Partition implements AutoCloseable {
    public static final int DATA_PAGES_PER_PARTITION = HEADER_PAGES_PER_MASTER * DATA_PAGES_PER_HEADER;
    ReentrantLock partLock;
    final int partNum;
    private RandomAccessFile file;
    private FileChannel fileChannel;
    // type of each entry is unsigned short. Use int instead since java does not support unsigned short
    private final int[] masterPage;
    private final byte[][] headerPages; // [HEADER_PAGES_PER_MASTER][PAGE_SIZE] = [HEADER_PAGES_PER_MASTER][DATA_PAGES_PER_HEADER bits]

    public Partition(int partNum){
        this.partNum = partNum;
        this.masterPage = new int[HEADER_PAGES_PER_MASTER];
        this.headerPages = new byte[HEADER_PAGES_PER_MASTER][];
        this.partLock = new ReentrantLock();
        init();
    }

    void init(){
        Arrays.fill(masterPage, 0);
        Arrays.fill(headerPages, null);
    }

    /**
     * Open the OS file backing this partition and load master/header pages into memory.
     **/
    void open(String fileName) {
        try{
            file = new RandomAccessFile(fileName, "rw");
            fileChannel = file.getChannel();
            if (fileChannel.size() == 0){
                // new file, write initial master page
                writeMasterPage();
            }else {
                // first page(pageNum=0) is always master page
                ByteBuffer masterBuffer = ByteBuffer.allocate(PAGE_SIZE);
                fileChannel.read(masterBuffer, Partition.masterPageOffset());
                masterBuffer.position(0);
                for (int headerIdx = 0; headerIdx < HEADER_PAGES_PER_MASTER; headerIdx++){
                    int allocNum = Short.toUnsignedInt(masterBuffer.getShort()); //make sure short is unsigned here, otherwise will overflow
                    if (allocNum != 0){
                        this.masterPage[headerIdx] = allocNum;
                        ByteBuffer headerBuffer = ByteBuffer.allocate(PAGE_SIZE);
                        fileChannel.read(headerBuffer, Partition.headerPageByteOffset(headerIdx));
                        headerPages[headerIdx] = headerBuffer.array();
                    }
                }
            }
        }catch (IOException e){
            throw new PageException();
        }
    }

    /**
     * Check if pageNum is legal.
     * */
    private void checkPageNum(int pageNum) {
        if (pageNum < 0 || pageNum >= DATA_PAGES_PER_PARTITION){
            final String PAGE_NUM_OOB_ERR = "Page number(%d) is out of the value range:[0, %d]";
            throw new PageException(String.format(PAGE_NUM_OOB_ERR, pageNum, DATA_PAGES_PER_PARTITION -1));
        }
    }

    /**
     * Implicitly called method, as a result should use lock.
     * */
    @Override
    public void close() throws IOException {
        partLock.lock();
        try{
            fileChannel.close();
            file.close();
        }finally {
            partLock.unlock();
        }
    }

    private void writeMasterPage() throws IOException {
        ByteBuffer masterBuffer = ByteBuffer.allocate(PAGE_SIZE);
        for (int i = 0; i < masterPage.length; i++){
            masterBuffer.putShort((short)masterPage[i]);
        }
        writePageHelper(masterPageOffset(), masterBuffer.array());
    }

    private void writeHeaderPage(int headerIndex) throws IOException {
        long byteOffset = headerPageByteOffset(headerIndex);
        writePageHelper(byteOffset, headerPages[headerIndex]);
    }

    /**
     * write the data page onto disk
     * @param pageNum: the logical page number in the scope of the partition
     * @param buf: the byte array in the page to be written
     * */
    private void writeDataPage(int pageNum, byte[] buf) throws IOException {
        long byteOffset = dataPageByteOffset(pageNum);
        writePageHelper(byteOffset, buf);
    }

    private void writePageHelper(long byteOffset, byte[] buf) throws IOException {
        fileChannel.position(byteOffset);
        fileChannel.write(ByteBuffer.wrap(buf));
    }

    /**
     * Allocate a data page within this partition following the steps below:
     *  step1. iterate through master page to find first header page with free data pages
     *  step2. iterate through each bit of header of step1 to find first free data page
     * @return logical page number of allocated data page.
     * */
    int allocPage() throws IOException {
        int headerIdx = -1;
        for (int i = 0; i < masterPage.length; i++){
            if (masterPage[i] < DATA_PAGES_PER_HEADER){
                headerIdx = i;
                break;
            }
        }
        if (headerIdx == -1){
            throw new PageException("No free data page to allocate.");
        }

        byte[] header = headerPages[headerIdx];
        int dataIdx = -1;
        if (header == null){
            // empty header means no data page allocated in this header yet.
            dataIdx = 0;
        }else{
            for (int i = 0; i < DATA_PAGES_PER_HEADER; i++){
                // Search for the first 0 bit in DATA_PAGES_PER_HEADER bits of the header
                if (Bits.getBit(header, i) == Bits.Bit.ZERO){
                    dataIdx = i;
                    break;
                }
            }
        }

        if (dataIdx == -1){
            throw new PageException("No available data page found, but there are %d data pages");
        }
        return allocPage(headerIdx, dataIdx);
    }

    int allocPage(int pageNum) throws IOException {
        int headerIdx = pageNum / DATA_PAGES_PER_HEADER;
        int dataIdx = pageNum % DATA_PAGES_PER_HEADER;
        return allocPage(headerIdx, dataIdx);
    }

    int allocPage(int headerIdx, int dataIdx) throws IOException {
        byte[] header = headerPages[headerIdx];
        if (header == null){
            header = headerPages[headerIdx] = new byte[PAGE_SIZE];
        }

        if (Bits.getBit(header, dataIdx) == Bits.Bit.ONE){
            throw new PageException("Data page is not free");
        }

        // data page@<headerIdx, dataIdx> is ready to be allocated at this point
        Bits.setBit(header, dataIdx, Bits.Bit.ONE);
        masterPage[headerIdx] = Bits.countBits(header);
        writeMasterPage();
        writeHeaderPage(headerIdx);
        //TODO: consider crash recovery.

        return headerIdx * DATA_PAGES_PER_HEADER + dataIdx;
    }

    /**
     * Free a page
     * @param pageNum: logical page number within this partition
     * */
    void freePage(int pageNum) throws IOException {
        if (isFreePage(pageNum)){
            throw new PageException("Cannot deallocate a free page");
        }
        int headerIdx = pageNum / DATA_PAGES_PER_HEADER;
        int dataIdx = pageNum % DATA_PAGES_PER_HEADER;
        freePage(headerIdx, dataIdx);
    }

    private void freePage(int headerIdx, int dataIdx) throws IOException {
        byte[] header = headerPages[headerIdx];
        Bits.setBit(header, dataIdx, Bits.Bit.ZERO);
        masterPage[headerIdx] = Bits.countBits(header);
        writeMasterPage();
        writeHeaderPage(headerIdx);
    }

    void readPage(int pageNum, byte[] buf) throws IOException {
        checkPageNum(pageNum);
        if (isFreePage(pageNum)){
            throw new PageException("Cannot read a free page");
        }

        fileChannel.read(ByteBuffer.wrap(buf), dataPageByteOffset(pageNum));
    }

    void writePage(int pageNum, byte[] buf) throws IOException {
        checkPageNum(pageNum);
        if (isFreePage(pageNum)){
            throw new PageException("Failed to write to page. It is not allocate.");
        }
        fileChannel.write(ByteBuffer.wrap(buf), dataPageByteOffset(pageNum));
        fileChannel.force(false); // No-Force & Steal policy.
    }

    boolean isFreePage(int pageNum) {
        checkPageNum(pageNum);
        int headerIdx = pageNum / DATA_PAGES_PER_HEADER;
        int dataIdx = pageNum % DATA_PAGES_PER_HEADER;
        byte[] header = headerPages[headerIdx];
        if (header == null || masterPage[headerIdx] == 0){
            return true;
        }
        return Bits.getBit(header, dataIdx) == Bits.Bit.ZERO;
    }

    /**
     * Deallocate all the data pages of this partition
     * */
    void freeDataPages() throws IOException {
        for (int headerIdx = 0; headerIdx < HEADER_PAGES_PER_MASTER; headerIdx++){
            if (masterPage[headerIdx] == 0){
                continue;
            }
            byte[] header = headerPages[headerIdx];
            for (int dataIdx = 0; dataIdx < DATA_PAGES_PER_PARTITION; dataIdx++){
                if (Bits.getBit(header, dataIdx) == Bits.Bit.ONE){
                    this.freePage(headerIdx, dataIdx);
                }
                if (Bits.countBits(header) == 0){
                    break;
                }
            }
        }
    }

    private static long masterPageOffset(){
        return 0;
    }

    /**
     * Convert header index to the page offset
     * */
    private static long headerPageByteOffset(int headerIndex){
        return (1 + (long) headerIndex * (DATA_PAGES_PER_HEADER + 1)) * PAGE_SIZE;
    }

    /**
     * Convert logical data pageNum within partition to the page offset
     */
    private static long dataPageByteOffset(int pageNum){
        return (long) (pageNum / DATA_PAGES_PER_HEADER + 2 + pageNum) * PAGE_SIZE;
    }

    int[] getMasterPage(){
        return masterPage;
    }

    byte[][] getHeaderPages(){
        return headerPages;
    }

    long getFileSize() throws IOException {
        return fileChannel.size();
    }
}
