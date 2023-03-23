package org.csfundamental.database.storage;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

public class DiskSpaceManagerImplTest {
    private static final String DIR = "diskspacemanager";
    private static final int INIT_PARTS = 3;
    private String dirPath;
    private File dsmDir;

    @Before
    public void setup() throws IOException {
        String resourceRoot = getClass().getClassLoader().getResource("").getPath();
        dsmDir = Paths.get(resourceRoot, DIR).toFile();
        if (!dsmDir.exists()){
            dsmDir.mkdirs();
        }
        dirPath = dsmDir.getAbsolutePath();
        for (int i = 0; i < INIT_PARTS; i++){
            File f = Paths.get(dirPath, String.valueOf(i)).toFile();
            if (!f.exists()){
                f.createNewFile();
            }
        }
    }

    private void clearPartitionFiles(){
        if (dsmDir.exists()){
            for (File file : dsmDir.listFiles()){
                file.delete();
            }
        }
    }

    @After
    public void reset_EmptyPartitionFile() throws IOException {
        clearPartitionFiles();
        if (dsmDir.exists()){
            dsmDir.delete();
        }
    }

    @Test
    public void createNewDiskSpaceManager() throws IOException {
        IDiskSpaceManager dsm = new DiskSpaceManagerImpl(dirPath);
        int partNum = dsm.allocPart();
        Assert.assertEquals(3, partNum);
    }

    @Test
    public void allocPartitionsWithMultipleThread() throws InterruptedException {
        IDiskSpaceManager dsm = new DiskSpaceManagerImpl(dirPath);
        int nThread = 10;
        int allocCount = 30;
        Thread[] allocPartWorkers = new Thread[nThread];
        for (int i = 0; i < allocPartWorkers.length; i++){
            allocPartWorkers[i] = new Thread(()->{
                try {
                    for (int j = 0; j < allocCount; j++){
                        dsm.allocPart();
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        for (Thread worker : allocPartWorkers){
            worker.start();
        }

        for (Thread worker: allocPartWorkers){
            worker.join();
        }

        Assert.assertEquals(nThread * allocCount + INIT_PARTS, dsm.getCurrentPartNum());
    }
}