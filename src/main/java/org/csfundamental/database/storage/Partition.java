package org.csfundamental.database.storage;

import org.csfundamental.database.common.Bits;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import static org.csfundamental.database.storage.DiskManagerImpl.HEADER_PAGES_PER_MASTER;
import static org.csfundamental.database.storage.DiskManagerImpl.DATA_PAGES_PER_HEADER;
import static org.csfundamental.database.storage.IDiskManager.PAGE_SIZE;

public class Partition implements Closeable {
    private int partNum;

    private RandomAccessFile file;

    private FileChannel fileChannel;

    // type of each entry is unsigned short. Use int instead since java does not support unsigned short
    private int[] masterPage;

    private byte[][] headerPages; // [HEADER_PAGES_PER_MASTER][PAGE_SIZE] = [HEADER_PAGES_PER_MASTER][DATA_PAGES_PER_HEADER bits]

    public Partition(int partNum){
        this.partNum = partNum;
        this.masterPage = new int[HEADER_PAGES_PER_MASTER];
        Arrays.fill(masterPage, (short)0);
        this.headerPages = new byte[HEADER_PAGES_PER_MASTER][];
    }

    /**
     * Open the file and load master/header pages into memory
     * */
    void open(String fileName) throws IOException {
        file = new RandomAccessFile(fileName, "rw");
        fileChannel = file.getChannel();
        long length = fileChannel.size();
        if (length == 0){
            // new file, write empty master page
            writeMasterPage();
        }else {
            // first page(pageNum=0) is always master page
            ByteBuffer masterBuf = ByteBuffer.allocate(PAGE_SIZE);
            fileChannel.read(masterBuf, Partition.masterPageOffset());
            masterBuf.position(0);
            for (int headerIdx = 0; headerIdx < HEADER_PAGES_PER_MASTER; headerIdx++){
                int allocNum = Short.toUnsignedInt(masterBuf.getShort());
                if (allocNum != 0){
                    this.masterPage[headerIdx] = allocNum;
                    ByteBuffer headerBuf = ByteBuffer.allocate(PAGE_SIZE);
                    fileChannel.read(headerBuf, Partition.headerPageByteOffset(headerIdx));
                    headerPages[headerIdx] = headerBuf.array();
                }
            }
        }
    }


    @Override
    public void close() throws IOException {
        if (fileChannel.isOpen()){
            fileChannel.close();
        }
        file.close();
    }

    private void writeMasterPage() throws IOException {
        ByteBuffer masterBuf = ByteBuffer.allocate(PAGE_SIZE);
        for (int i = 0; i < masterPage.length; i++){
            masterBuf.putShort((short)masterPage[i]);
        }
        writePage(masterPageOffset(), masterBuf.array());
    }

    private void writeHeaderPage(int headerIndex) throws IOException {
        long byteOffset = headerPageByteOffset(headerIndex);
        writePage(byteOffset, headerPages[headerIndex]);
    }

    /**
     * flush data of page with pageNum onto disk
     * @param pageNum: the logical page number within the page
     * @param buf: the byte array to be flushed to
     * */
    private void writeDataPage(int pageNum, byte[] buf) throws IOException {
        long byteOffset = dataPageByteOffset(pageNum);
        writePage(byteOffset, buf);
    }

    private void writePage(long byteOffset, byte[] buf) throws IOException {
        fileChannel.position(byteOffset);
        fileChannel.write(ByteBuffer.wrap(buf));
    }

    /**
     * Allocate a data page within this partition
     *
     * @return logical page number
     * */
    int allocPage() throws Exception {
        //step1. loop through master to find first header with free data page
        //step2. loop through header of step1 to find first free data page
        int headerIdx = -1;
        for (int i = 0; i < masterPage.length; i++){
            if (masterPage[i] < DATA_PAGES_PER_HEADER){
                headerIdx = i;
                break;
            }
        }
        if (headerIdx == -1){
            throw new Exception("No free data page to allocate.");
        }

        byte[] header = headerPages[headerIdx];
        int dataIdx = -1;
        if (header == null){
            // empty header means no data page allocated in this header yet.
            dataIdx = 0;
        }else{
            for (int i = 0; i < DATA_PAGES_PER_HEADER; i++){
                // Search for the first 0 bit. header.length = DATA_PAGES_PER_HEADER bits = PAGE_SIZE.
                if (Bits.getBit(header, i) == Bits.Bit.ZERO){
                    dataIdx = i;
                    break;
                }
            }
        }

        if (dataIdx == -1){
            throw new Exception("No available data page found, but there are %d data pages");
        }
        return allocPage(headerIdx, dataIdx);
    }

    int allocPage(int headerIdx, int dataIdx) throws Exception {
        byte[] header = headerPages[headerIdx];
        if (header == null){
            header = headerPages[headerIdx] = new byte[PAGE_SIZE];
        }

        if (Bits.getBit(header, dataIdx) == Bits.Bit.ONE){
            throw new Exception("Data page is not free");
        }

        // data page@<headerIdx, dataIdx> is ready to be allocated at this point
        masterPage[headerIdx]++;
        Bits.setBit(header, dataIdx, Bits.Bit.ONE);
        writeMasterPage();
        writeHeaderPage(headerIdx);
        return headerIdx * DATA_PAGES_PER_HEADER + dataIdx;
    }

    /**
     * Free a page
     * @param pageNum: logical page number within this partition
     * */
    void freePage(int pageNum) throws Exception {
        int headerIdx = pageNum / DATA_PAGES_PER_HEADER;
        int dataIdx = pageNum % DATA_PAGES_PER_HEADER;
        byte[] header = headerPages[headerIdx];
        if (Bits.getBit(header, dataIdx) == Bits.Bit.ZERO){
            throw new Exception("Cannot deallocate a free page");
        }
        //TODO: continue deallocate page
    }

    void readPage(int pageNum, byte[] buf){

    }

    boolean isFreePage(int pageNum){
        return false;
    }

    void freeDataPages(){

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
}
