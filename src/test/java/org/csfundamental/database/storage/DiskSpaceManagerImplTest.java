package org.csfundamental.database.storage;

import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

public class DiskSpaceManagerImplTest {
    private static final String DIR = "diskspacemanager";
    private String dirPath;
    private File dsmRootDir;
    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Before
    public void beforeEach() throws IOException {
        dsmRootDir = tmpFolder.newFolder(DIR);
        dirPath = dsmRootDir.getAbsolutePath();
    }

    private IDiskSpaceManager createNewDiskSpaceManager() {
        return new DiskSpaceManagerImpl(dirPath);
    }

    @Test
    public void allocPartitionsWithMultipleThread() throws InterruptedException {
        IDiskSpaceManager dsm = createNewDiskSpaceManager();
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

        Assert.assertEquals(nThread * allocCount, dsm.getCurrentPartNum());
    }
}