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
    private final int partNum;

    private RandomAccessFile file;

    private FileChannel fileChannel;

    // type of each entry is unsigned short. Use int instead since java does not support unsigned short
    private final int[] masterPage;

    private final byte[][] headerPages; // [HEADER_PAGES_PER_MASTER][PAGE_SIZE] = [HEADER_PAGES_PER_MASTER][DATA_PAGES_PER_HEADER bits]

    public Partition(int partNum){
        this.partNum = partNum;
        this.masterPage = new int[HEADER_PAGES_PER_MASTER];
        this.headerPages = new byte[HEADER_PAGES_PER_MASTER][];
    }

    /**
     * Open the OS file backing this partition and load master/header pages into memory
     * */
    void open(String fileName) throws IOException {
        file = new RandomAccessFile(fileName, "rw");
        fileChannel = file.getChannel();
        if (fileChannel.size() == 0){
            // new file, write the empty master page
            writeMasterPage();
        }else {
            // first page(pageNum=0) is always master page
            ByteBuffer masterBuffer = ByteBuffer.allocate(PAGE_SIZE);
            fileChannel.read(masterBuffer, Partition.masterPageOffset());
            masterBuffer.position(0);
            for (int headerIdx = 0; headerIdx < HEADER_PAGES_PER_MASTER; headerIdx++){
                int allocNum = Short.toUnsignedInt(masterBuffer.getShort());
                if (allocNum != 0){
                    this.masterPage[headerIdx] = allocNum;
                    ByteBuffer headerBuffer = ByteBuffer.allocate(PAGE_SIZE);
                    fileChannel.read(headerBuffer, Partition.headerPageByteOffset(headerIdx));
                    headerPages[headerIdx] = headerBuffer.array();
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
        ByteBuffer masterBuffer = ByteBuffer.allocate(PAGE_SIZE);
        for (int i = 0; i < masterPage.length; i++){
            masterBuffer.putShort((short)masterPage[i]);
        }
        writePage(masterPageOffset(), masterBuffer.array());
    }

    private void writeHeaderPage(int headerIndex) throws IOException {
        long byteOffset = headerPageByteOffset(headerIndex);
        writePage(byteOffset, headerPages[headerIndex]);
    }

    /**
     * write the data page onto disk
     * @param pageNum: the logical page number in the scope of the partition
     * @param buf: the byte array in the page to be written
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
     * Allocate a data page within this partition following the steps below:
     *  step1. iterate through master page to find first header page with free data pages
     *  step2. iterate through each bit of header of step1 to find first free data page
     * @return logical page number of allocated data page.
     * */
    int allocPage() throws Exception {
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
                // Search for the first 0 bit in DATA_PAGES_PER_HEADER bits of the header
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
        if (isFreePage(pageNum)){
            throw new Exception("Cannot deallocate a free page");
        }
        int headerIdx = pageNum / DATA_PAGES_PER_HEADER;
        masterPage[headerIdx]--;
        byte[] header = headerPages[headerIdx];
        int dataIdx = pageNum % DATA_PAGES_PER_HEADER;
        Bits.setBit(header, dataIdx, Bits.Bit.ZERO);
        writeMasterPage();
        writeHeaderPage(headerIdx);
    }

    void readPage(int pageNum, byte[] buf) throws Exception {
        if (isFreePage(pageNum)){
            throw new Exception("Cannot read a free page");
        }

        fileChannel.read(ByteBuffer.wrap(buf), dataPageByteOffset(pageNum));
    }

    boolean isFreePage(int pageNum){
        int headerIdx = pageNum / DATA_PAGES_PER_HEADER;
        int dataIdx = pageNum % DATA_PAGES_PER_HEADER;
        byte[] header = headerPages[headerIdx];
        if (header == null){
            return true;
        }
        return Bits.getBit(header, dataIdx) == Bits.Bit.ZERO;
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
