package org.csfundamental.database.storage;

import org.csfundamental.database.common.Bits;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import static org.csfundamental.database.storage.DiskManagerImpl.DATA_PAGES_PER_HEADER;
import static org.csfundamental.database.storage.DiskManagerImpl.HEADER_PAGES_PER_MASTER;
import static org.csfundamental.database.storage.IDiskManager.PAGE_SIZE;

public class Partition implements Closeable {
    private int partNum;
    private RandomAccessFile file;
    private FileChannel fileChannel;
    private short[] master;
    private byte[][] headers;

    public Partition(int partNum){
        this.partNum = partNum;
        this.master = new short[PAGE_SIZE];
        Arrays.fill(master, (short)0);
        this.headers = new byte[HEADER_PAGES_PER_MASTER][];
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
            flushMasterPage();
        }else {
            ByteBuffer masterBuf = ByteBuffer.allocate(PAGE_SIZE);
            fileChannel.read(masterBuf);
            int headerIndex = 0;
            while(masterBuf.hasRemaining()){
                short allocNum = masterBuf.getShort();
                ByteBuffer headerBuf = ByteBuffer.allocate(PAGE_SIZE);
                fileChannel.read(headerBuf);
                headers[headerIndex] = headerBuf.array();
                headerIndex++;
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

    private void flushMasterPage() throws IOException {
        ByteBuffer masterBuf = ByteBuffer.allocate(PAGE_SIZE);
        for (int i = 0; i < HEADER_PAGES_PER_MASTER; i++){
            masterBuf.putShort(master[i]);
        }

        flushPage(masterPageOffset(), masterBuf.array());
    }

    private void flushHeaderPage(int headerIndex) throws IOException {
        long byteOffset = headerPageByteOffset(headerIndex);
        flushPage(byteOffset, headers[headerIndex]);
    }

    /**
     * flush data of page with pageNum onto disk
     * @param pageNum: the logical page number within the page
     * @param buf: the byte array to be flushed to
     * */
    private void flushDataPage(int pageNum, byte[] buf) throws IOException {
        long byteOffset = dataPageByteOffset(pageNum);
        flushPage(byteOffset, buf);
    }

    private void flushPage(long byteOffset, byte[] buf) throws IOException {
        fileChannel.position(byteOffset);
        fileChannel.write(ByteBuffer.wrap(buf));
    }

    int allocPage(){
        return -1;
    }

    void freePage(int pageNum){

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
}
