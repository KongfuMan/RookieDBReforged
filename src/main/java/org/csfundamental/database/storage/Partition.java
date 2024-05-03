package org.csfundamental.database.storage;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.locks.ReentrantLock;

import static org.csfundamental.database.storage.DiskSpaceManagerImpl.HEADER_PAGES_PER_MASTER;
import static org.csfundamental.database.storage.DiskSpaceManagerImpl.DATA_PAGES_PER_HEADER;
import static org.csfundamental.database.storage.DiskSpaceManager.PAGE_SIZE;

/**
 * One partition is backed by one OS file.
 * The methods are not thread-safe. It is the caller's responsibility to use partLock to protect critical section.
 * Partition exposes logic page number of data page.
 * */
public class Partition implements AutoCloseable {
    public static final int DATA_PAGES_PER_PARTITION = HEADER_PAGES_PER_MASTER * DATA_PAGES_PER_HEADER;
    ReentrantLock partLock;
    private final int partNum;
    private RandomAccessFile file;
    private FileChannel fileChannel;

    // type of each entry is unsigned short. Use int instead since java does not support unsigned short
    private final int[] masterPage;
    private final BitSet[] headerPages;

    Partition(int partNum){
        this.partNum = partNum;
        this.masterPage = new int[HEADER_PAGES_PER_MASTER];
        this.headerPages = new BitSet[HEADER_PAGES_PER_MASTER];
        this.partLock = new ReentrantLock();
        reset();
    }

    void reset(){
        Arrays.fill(masterPage, 0);
        for (int i = 0; i < this.headerPages.length; i++){
            this.headerPages[i] = new BitSet(0);
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

    /**
     * Open the OS file backing this partition and load master/header pages into memory.
     **/
    void loadFromFile(String fileName) {
        try{
            file = new RandomAccessFile(fileName, "rw");
            fileChannel = file.getChannel();
            if (fileChannel.size() == 0){
                // new file, write initial master page
                writeMasterPage();
            }else {
                ByteBuffer masterBuffer = ByteBuffer.allocate(PAGE_SIZE);
                fileChannel.read(masterBuffer, Partition.masterPageOffset());
                masterBuffer.position(0);
                for (int headerIdx = 0; headerIdx < HEADER_PAGES_PER_MASTER; headerIdx++){
                    int allocNum = Short.toUnsignedInt(masterBuffer.getShort()); //make sure short is unsigned here, otherwise will overflow
                    if (allocNum != 0){
                        this.masterPage[headerIdx] = allocNum;
                        ByteBuffer headerBuffer = ByteBuffer.allocate(PAGE_SIZE);
                        headerBuffer.clear();
                        fileChannel.read(headerBuffer, Partition.headerPageByteOffset(headerIdx));
                        headerPages[headerIdx] = BitSet.valueOf(headerBuffer.array());
                    }
                }
            }
        }catch (IOException e){
            throw new PageException();
        }
    }

    int getPartNum() {
        return this.partNum;
    }

    /**
     * Allocate a data page within this partition following the steps below:
     *  step1. iterate through master page to find first header page with free data pages
     *  step2. iterate through each bit of header of step1 to find first free data page
     * @return logical page number of allocated data page.
     * */
    int allocPage() throws IOException {
        int headerIdx = firstHeaderIndexWithFreeDataPage();
        if (headerIdx == -1){
            throw new PageException("Partition is full.");
        }

        BitSet header = getHeaderPage(headerIdx);
        assert header.cardinality() < DATA_PAGES_PER_HEADER;
        int dataIdx = header.nextClearBit(0);
        return doAllocPage(headerIdx, dataIdx);
    }

    /**
     * Allocate a data page at specific logic page number within the partition.
     * @param pageNum: logical page number within this partition
     * */
    void allocPage(int pageNum) throws IOException {
        if (!isFreePage(pageNum)){
            throw new PageException(String.format("Cannot allocate page(%d) in use.", pageNum));
        }

        int headerIdx = pageNum / DATA_PAGES_PER_HEADER;
        int dataIdx = pageNum % DATA_PAGES_PER_HEADER;
        doAllocPage(headerIdx, dataIdx);
    }

    /**
     * Free a page
     * @param pageNum: logical page number within this partition
     * */
    void freePage(int pageNum) throws IOException {
        if (isFreePage(pageNum)){
            throw new PageException("Cannot deallocate a free page.");
        }
        int headerIdx = pageNum / DATA_PAGES_PER_HEADER;
        int dataIdx = pageNum % DATA_PAGES_PER_HEADER;
        doFreePage(headerIdx, dataIdx);
    }


    /**
     * Read a page from disk.
     * @param pageNum: logical page number within this partition
     * @param buf: destination byte array.
     * */
    void readPage(int pageNum, byte[] buf) throws IOException {
        if (isFreePage(pageNum)){
            throw new PageException("Cannot read a free page");
        }

        fileChannel.read(ByteBuffer.wrap(buf), dataPageByteOffset(pageNum));
    }

    /**
     * Write a page from disk.
     * @param pageNum: logical page number within this partition
     * @param buf: destination byte array.
     * */
    void writePage(int pageNum, byte[] buf) throws IOException {
        if (isFreePage(pageNum)){
            throw new PageException("Failed to write to page. It is not allocate.");
        }
        fileChannel.write(ByteBuffer.wrap(buf), dataPageByteOffset(pageNum));
        fileChannel.force(false); // No-Force & Steal policy.
    }

    /**
     * Check whether a data page is free to allocate.
     * @param pageNum: logical page number within this partition
     * */
    boolean isFreePage(int pageNum) {
        checkPageNum(pageNum);
        int headerIdx = pageNum / DATA_PAGES_PER_HEADER;
        int dataIdx = pageNum % DATA_PAGES_PER_HEADER;
        return isFreePage(headerIdx, dataIdx);
    }

    /**
     * Deallocate all the data pages of the partition
     * */
    void freeAllPages() throws IOException {
        for (int headerIdx = 0; headerIdx < HEADER_PAGES_PER_MASTER; headerIdx++){
            BitSet header = getHeaderPage(headerIdx);
            header.clear();
            masterPage[headerIdx] = 0;
            writeHeaderPage(headerIdx);
        }
        writeMasterPage();
    }

    private static long masterPageOffset(){
        return 0;
    }

    /**
     * Convert index of header into its byte offset
     * */
    private static long headerPageByteOffset(int headerIndex){
        return (1 + (long) headerIndex * (DATA_PAGES_PER_HEADER + 1)) * PAGE_SIZE;
    }

    /**
     * Convert logical pageNum of data page to its byte offset
     */
    private static long dataPageByteOffset(int pageNum){
        return (long) (pageNum / DATA_PAGES_PER_HEADER + 2 + pageNum) * PAGE_SIZE;
    }

    private void writeMasterPage() throws IOException {
        ByteBuffer masterBuffer = ByteBuffer.allocate(PAGE_SIZE);
        for (int i = 0; i < masterPage.length; i++){
            masterBuffer.putShort((short)masterPage[i]);
        }
        doWritePage(masterPageOffset(), masterBuffer.array());
    }

    private void writeHeaderPage(int headerIndex) throws IOException {
        long byteOffset = headerPageByteOffset(headerIndex);
        doWritePage(byteOffset, headerPages[headerIndex].toByteArray());
    }

    /**
     * write the data page onto disk
     * @param pageNum: the logical page number in the scope of the partition
     * @param buf: the byte array in the page to be written
     * */
    private void writeDataPage(int pageNum, byte[] buf) throws IOException {
        long byteOffset = dataPageByteOffset(pageNum);
        doWritePage(byteOffset, buf);
    }

    private void doWritePage(long byteOffset, byte[] buf) throws IOException {
        fileChannel.position(byteOffset);
        fileChannel.write(ByteBuffer.wrap(buf));
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

    private int firstHeaderIndexWithFreeDataPage(){
        int headerIdx = -1;
        for (int i = 0; i < masterPage.length; i++){
            if (masterPage[i] < DATA_PAGES_PER_HEADER){
                headerIdx = i;
                break;
            }
        }
        return headerIdx;
    }

    private BitSet getHeaderPage(int headerIdx){
        return this.headerPages[headerIdx];
    }

    /**
     * private method, caller should check if data page {headerIdx, dataIdx} is free.
     * */
    private int doAllocPage(int headerIdx, int dataIdx) throws IOException {
        BitSet header = getHeaderPage(headerIdx);
        header.set(dataIdx, true);
        masterPage[headerIdx] = header.cardinality();
        writeMasterPage();
        writeHeaderPage(headerIdx);
        //TODO: consider crash recovery.

        return headerIdx * DATA_PAGES_PER_HEADER + dataIdx;
    }

    private void doFreePage(int headerIdx, int dataIdx) throws IOException {
        BitSet header = getHeaderPage(headerIdx);
        header.clear(dataIdx);
        masterPage[headerIdx] =  header.cardinality();
        writeMasterPage();
        writeHeaderPage(headerIdx);
    }

    private boolean isFreePage(int headerIdx, int dataIdx){
        if (masterPage[headerIdx] == DATA_PAGES_PER_HEADER){
            return false;
        }

        BitSet header = getHeaderPage(headerIdx);
        if (header.cardinality() == DATA_PAGES_PER_HEADER){
            return false;
        }
        return !header.get(dataIdx);
    }

    int[] getMasterPage(){
        return masterPage;
    }

    BitSet[] getHeaderPages(){
        return headerPages;
    }

    long getFileSize() throws IOException {
        return fileChannel.size();
    }
}
