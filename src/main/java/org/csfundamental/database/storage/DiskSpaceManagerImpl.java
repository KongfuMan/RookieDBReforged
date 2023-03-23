package org.csfundamental.database.storage;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages a collection of partitions.
 * (Singleton: only one instance per process)
 * */
public class DiskSpaceManagerImpl implements IDiskSpaceManager {
    private final Map<Integer, Partition> partMap;
    private final AtomicInteger partNumCounter;
    private final ReentrantLock managerLock;
    private final String dir;

    /**
     * Number of header pages included in one master page: PAGE_SIZE / 2 byte
     * */
    public static final int HEADER_PAGES_PER_MASTER = PAGE_SIZE / 2;

    /**
     * Number of data pages included in one header page: PAGE_SIZE / 1 bit
     * */
    public static final int DATA_PAGES_PER_HEADER = PAGE_SIZE * 8;
    public DiskSpaceManagerImpl(String dir){
        this.partMap = new HashMap<>();
        this.partNumCounter = new AtomicInteger(0);
        this.dir = dir;
        this.managerLock = new ReentrantLock();

        // TODO: create partition from files under the this.dir
        File f = new File(dir);
        if (f.exists()){
            for (File part : f.listFiles()){
                String fileName = part.getName();
                try{
                    int partNum = Integer.parseInt(fileName);
                    allocPart(partNum);
                }catch (NumberFormatException | IOException e){
                    part.delete();
                    throw new PageException("Create partition failed.");
                }
            }
        }
    }
    @Override
    public int allocPart() throws IOException {
        return allocPartHelper(this.partNumCounter.getAndIncrement());
    }
    @Override
    public int allocPart(int partNum) throws IOException {
        // atomically update current counter to next available partition number
        partNumCounter.updateAndGet((int maxPartNum) -> Math.max(maxPartNum, partNum) + 1);
        return allocPartHelper(partNum);
    }
    private int allocPartHelper(int partNum) throws IOException {
        Partition part;
        managerLock.lock();
        try{
            if (partMap.containsKey(partNum)){
                throw new PageException(String.format("Partition number (%d) is already in use", partNum));
            }
            part = new Partition(partNum);
            partMap.put(partNum, part);
        }finally {
            managerLock.unlock();
        }

        part.partLock.lock();
        try{
            part.open((Paths.get(dir, String.valueOf(partNum))).toString());
        }finally {
            part.partLock.unlock();
        }
        return partNum;
    }
    @Override
    public void freePart(int partNum) throws IOException {
        Partition part;
        managerLock.lock();
        try{
            part = getPartitionByPartNum(partNum);
            partMap.remove(part.partNum);
        }finally {
            managerLock.unlock();
        }

        part.partLock.lock();
        try{
            part.freeDataPages();
            part.init();
            part.close();

            File pf = new File(dir + "/" + partNum);
            if(!pf.delete()){
                throw new RuntimeException("Failed to delete the partition file.");
            }
        }finally {
            part.partLock.unlock();
        }
    }

    /**
     * Allocate a page in a specific partition
     *
     * @param partNum: number of partition in which page is allocated.
     * @return virtual page number scoped to disk space manager.
     * */
    @Override
    public long allocPage(int partNum) throws IOException {
        Partition part;
        managerLock.lock();
        try{
            part = getPartitionByPartNum(partNum);
        }finally {
            managerLock.unlock();
        }

        part.partLock.lock();
        try{
            int pageNum = part.allocPage();
            return IDiskSpaceManager.getVirtualPageNum(partNum, pageNum);
        }finally {
            part.partLock.unlock();
        }
    }

    /**
     * Allocate a page of specific virtual page number
     * @param page: virtual page number
     * */
    @Override
    public long allocPage(long page) throws IOException {
        Partition part;
        managerLock.lock();
        try{
            part = getPartitionByPageNum(page);
        }finally {
            managerLock.unlock();
        }

        int pageNum = IDiskSpaceManager.getPageNum(page);
        part.partLock.lock();
        try{
            part.allocPage(pageNum);
            return page;
        }finally {
            part.partLock.unlock();
        }
    }
    @Override
    public void freePage(long page) throws IOException {
        Partition part;
        managerLock.lock();
        try {
            part = getPartitionByPageNum(page);
        }finally {
            managerLock.unlock();
        }

        int pageNum = IDiskSpaceManager.getPageNum(page);
        part.partLock.lock();
        try{
            part.freePage(pageNum);
        }finally {
            part.partLock.unlock();
        }
    }
    @Override
    public void readPage(long page, byte[] buf) throws IOException {
        if (buf.length != PAGE_SIZE){
            throw new IllegalArgumentException("Write page expects a page-sized buffer.");
        }
        int pageNum = IDiskSpaceManager.getPageNum(page);
        managerLock.lock();
        Partition part;
        try{
            part = getPartitionByPageNum(page);
        }finally {
            managerLock.unlock();
        }

        try{
            part.partLock.lock();
            part.readPage(pageNum, buf);
        }finally {
            part.partLock.unlock();
        }
    }
    @Override
    public void writePage(long page, byte[] buf) throws IOException {
        if (buf.length != PAGE_SIZE){
            throw new IllegalArgumentException("Write page expects a page-sized buffer.");
        }
        int pageNum = IDiskSpaceManager.getPageNum(page);
        Partition part;
        managerLock.lock();
        try{
            part = getPartitionByPageNum(page);
        }finally {
            managerLock.unlock();
        }

        try {
            part.partLock.lock();
            part.writePage(pageNum, buf);
        }finally {
            part.partLock.unlock();
        }
    }

    @Override
    public int getCurrentPartNum() {
        return partNumCounter.get();
    }
    private Partition getPartitionByPartNum(int partNum){
        if (!partMap.containsKey(partNum)){
            throw new NoSuchElementException("Failed to get partition, it does not exit.");
        }
        return partMap.get(partNum);
    }
    private Partition getPartitionByPageNum(long page){
        int partNum = IDiskSpaceManager.getPartNum(page);
        return getPartitionByPartNum(partNum);
    }

    @Override
    public void close() throws IOException {
        for (Partition part : partMap.values()){
            part.close();
        }
    }
}
