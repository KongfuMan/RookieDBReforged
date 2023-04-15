package org.csfundamental.database.storage;

import java.io.IOException;
import java.util.Map;

public class MemoryDiskSpanceManager implements DiskSpaceManager{
    private Map<Long, byte[]> dataMap;

    @Override
    public int allocPart() {
        return 0;
    }

    @Override
    public int allocPart(int partNum) {
        return 0;
    }

    @Override
    public void freePart(int partNum) {

    }

    @Override
    public long allocPage(int partNum) {
        return 0;
    }

    @Override
    public long allocPage(long page) {
        return 0;
    }

    @Override
    public void freePage(long page) {

    }

    @Override
    public void readPage(long page, byte[] data) {

    }

    @Override
    public void writePage(long page, byte[] data) {

    }

    @Override
    public boolean pageAllocated(long page) {
        return false;
    }

    @Override
    public int getCurrentPartNum() {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
