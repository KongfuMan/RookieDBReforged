package org.csfundamental.database.storage;

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
public class DiskSpaceManagerImpl implements DiskSpaceManager {
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
                }catch (NumberFormatException e){
                    part.delete();
                    throw new PageException("Create partition failed.");
                }
            }
        }
    }
    @Override
    public int allocPart() {
        return allocPartHelper(this.partNumCounter.getAndIncrement());
    }
    @Override
    public int allocPart(int partNum) {
        // atomically update current counter to next available partition number
        partNumCounter.updateAndGet((int maxPartNum) -> Math.max(maxPartNum, partNum) + 1);
        return allocPartHelper(partNum);
    }
    private int allocPartHelper(int partNum) {
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
    public void freePart(int partNum) {
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
        }catch (IOException e){
            throw new PageException("");
        }
        finally {
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
    public long allocPage(int partNum) {
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
            return DiskSpaceManager.getVirtualPageNum(partNum, pageNum);
        }catch (IOException e){
            throw new PageException("");
        }
        finally {
            part.partLock.unlock();
        }
    }

    /**
     * Allocate a page of specific virtual page number
     * @param page: virtual page number
     * */
    @Override
    public long allocPage(long page) {
        Partition part;
        managerLock.lock();
        try{
            part = getPartitionByPageNum(page);
        }finally {
            managerLock.unlock();
        }

        int pageNum = DiskSpaceManager.getPageNum(page);
        part.partLock.lock();
        try{
            part.allocPage(pageNum);
            return page;
        }catch (IOException e){
            throw new PageException("");
        }
        finally {
            part.partLock.unlock();
        }
    }
    @Override
    public void freePage(long page){
        Partition part;
        managerLock.lock();
        try {
            part = getPartitionByPageNum(page);
        }finally {
            managerLock.unlock();
        }

        int pageNum = DiskSpaceManager.getPageNum(page);
        part.partLock.lock();
        try{
            part.freePage(pageNum);
        }catch (IOException e){
            throw new PageException("");
        }
        finally {
            part.partLock.unlock();
        }
    }
    @Override
    public void readPage(long page, byte[] buf) {
        if (buf.length != PAGE_SIZE){
            throw new IllegalArgumentException("Write page expects a page-sized buffer.");
        }
        int pageNum = DiskSpaceManager.getPageNum(page);
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
        }catch (IOException e){
            throw new PageException("");
        }
        finally {
            part.partLock.unlock();
        }
    }
    @Override
    public void writePage(long page, byte[] buf) {
        if (buf.length != PAGE_SIZE){
            throw new IllegalArgumentException("Write page expects a page-sized buffer.");
        }
        int pageNum = DiskSpaceManager.getPageNum(page);
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
        }catch (IOException e){
            throw new PageException("");
        }
        finally {
            part.partLock.unlock();
        }
    }

    @Override
    public boolean pageAllocated(long page) {
        int partNum = DiskSpaceManager.getPartNum(page);
        int pageNum = DiskSpaceManager.getPageNum(page);
        managerLock.lock();
        Partition partition;
        try {
            partition = getPartitionByPartNum(partNum);
        }finally {
            managerLock.unlock();
        }

        partition.partLock.lock();
        try {
            return !partition.isFreePage(pageNum);
        }finally {
            partition.partLock.unlock();
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
        int partNum = DiskSpaceManager.getPartNum(page);
        return getPartitionByPartNum(partNum);
    }

    @Override
    public void close(){
        for (int partNum : partMap.keySet()){
            try{
                Partition part = partMap.get(partNum);
                part.close();
            }catch (IOException e){
                throw new PageException("could not close partition " + partNum + ": " + e.getMessage());
            }
        }
    }
}
