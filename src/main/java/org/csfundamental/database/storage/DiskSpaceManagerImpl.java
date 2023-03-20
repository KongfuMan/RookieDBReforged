package org.csfundamental.database.storage;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class DiskSpaceManagerImpl implements IDiskSpaceManager {
    private Map<Integer, Partition> partMap;

    private AtomicInteger partNumCounter;

    private ReentrantLock managerLock;

    private String dir;

    /**
     * Number of header pages included in one master page: PAGE_SIZE / 2 byte
     * */
    public static final int HEADER_PAGES_PER_MASTER = PAGE_SIZE / 2;

    /**
     * Number of data pages included in one header page: PAGE_SIZE / 1 bit
     * */
    public static final int DATA_PAGES_PER_HEADER = PAGE_SIZE * 8;

    public DiskSpaceManagerImpl(String dir){
        partMap = new ConcurrentHashMap<>();
        partNumCounter = new AtomicInteger(0);
        this.dir = dir;
    }

    @Override
    public int allocPart() throws Exception {
        return allocPartHelper(this.partNumCounter.getAndIncrement());
    }

    @Override
    public int allocPart(int partNum) throws Exception {
        if (partMap.containsKey(partNum)){
            throw new Exception("Partition number already exist.");
        }
        this.allocPartHelper(partNum);
        return partNum;
    }

    private int allocPartHelper(int partNum) throws Exception {
        Partition pi;
        managerLock.lock();
        try{
            if (partMap.containsKey(partNum)){
                throw new Exception("");
            }
            pi = new Partition(partNum);
            partMap.put(partNum, pi);
        }finally {
            managerLock.unlock();
        }
        pi.partLock.lock();
        try{
            pi.open((Paths.get(dir, String.valueOf(partNum))).toString());
        }finally {
            pi.partLock.unlock();
        }
        return pi.partNum;
    }

    @Override
    public void freePart(int partNum) throws Exception {
        if (partMap.containsKey((partNum))){
            partMap.get(partNum).freeDataPages();
        }
    }

    @Override
    public long allocPage(int partNum) throws Exception {
        Partition pi = getPartition(partNum);
        int pageNum = pi.allocPage();
        return IDiskSpaceManager.getVirtualPageNum(pi.partNum, pageNum);
    }

    @Override
    public long allocPage(long page) throws IOException {
        int partNum = IDiskSpaceManager.getPartNum(page);
        int pageNum = IDiskSpaceManager.getPageNum(page);
        Partition part = getPartition(partNum);
        part.allocPage(pageNum);
        return page;
    }

    @Override
    public void freePage(long page) throws Exception {
        int partNum = IDiskSpaceManager.getPartNum(page);
        int pageNum = IDiskSpaceManager.getPageNum(page);
        Partition part = getPartition(partNum);
        part.freePage(pageNum);
    }

    @Override
    public void readPage(long page, byte[] data) throws IOException {
        int partNum = IDiskSpaceManager.getPartNum(page);
        int pageNum = IDiskSpaceManager.getPageNum(page);
        Partition part = getPartition(partNum);
        part.readPage(pageNum, data);
    }

    @Override
    public void writePage(long page, byte[] data) throws IOException {
        int partNum = IDiskSpaceManager.getPartNum(page);
        int pageNum = IDiskSpaceManager.getPageNum(page);
        Partition part = getPartition(partNum);
        part.writePage(pageNum, data);
    }

    private Partition getPartition(int partNum){
        if (!partMap.containsKey(partNum)){
            throw new NoSuchElementException("Failed to get partition, it does not exit.");
        }
        return partMap.get(partNum);
    }
}
