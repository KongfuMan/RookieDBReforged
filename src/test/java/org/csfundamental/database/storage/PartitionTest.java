package org.csfundamental.database.storage;

import org.csfundamental.database.common.Bits;
import org.junit.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;

import static org.csfundamental.database.storage.DiskManagerImpl.DATA_PAGES_PER_HEADER;
import static org.csfundamental.database.storage.DiskManagerImpl.HEADER_PAGES_PER_MASTER;
import static org.csfundamental.database.storage.IDiskManager.PAGE_SIZE;

public class PartitionTest {
    private static final String PARTITION_PATH = "partitions/EmptyPartition";

    private String emptyFile;

    @Before
    public void setup() throws IOException {
        emptyFile = getClass().getClassLoader().getResource(PARTITION_PATH).getFile();
        reset_EmptyPartitionFile();
    }

    @After
    public void reset_EmptyPartitionFile() throws IOException {
        File f = new File(emptyFile);
        if (f.exists() && f.length() > 0){
            FileWriter fw = new FileWriter(f);
            fw.write("");
            fw.flush();
            fw.close();
        }
    }

    @Test
    public void alloc_DataPages_Under_First2HeaderPage() throws Exception {
        Partition part1 = new Partition(0);
        part1.open(emptyFile);
        int expectedPageNum = 0;
        for (int i = 0; i < 2 * DATA_PAGES_PER_HEADER; i += 32){
            int pageNum = part1.allocPage();
            Assert.assertEquals(pageNum, expectedPageNum++);
            Partition part2 = new Partition(0);
            part2.open(emptyFile);
            comparePartition(part1, part2);
        }
    }

    @Test
    public void alloc_DataPages_Under_FirstNHeaderPage() throws Exception {
        Partition part1 = new Partition(0);
        part1.open(emptyFile);
        for (int hIdx = 0; hIdx < HEADER_PAGES_PER_MASTER; hIdx += 32){
            for (int dIdx = 0; dIdx < DATA_PAGES_PER_HEADER; dIdx += DATA_PAGES_PER_HEADER - 1){
                part1.allocPage(hIdx, dIdx);
                Partition part2 = new Partition(0);
                part2.open(emptyFile);
                comparePartition(part1, part2);
            }
        }
    }

    @Test
    public void dealloc_DataPages() throws Exception {
        Partition part1 = new Partition(0);
        part1.open(emptyFile);
        int allocCount = 10;
        for (int i = 0; i < allocCount; i++){
            int pageNum = part1.allocPage();
            Assert.assertEquals(pageNum, i);
            Partition part2 = new Partition(0);
            part2.open(emptyFile);
            comparePartition(part1, part2);
        }

        for (int i = 0; i < allocCount - 1; i++){
            part1.freePage(i);
            Partition part2 = new Partition(0);
            part2.open(emptyFile);
            comparePartition(part1, part2);
        }

        part1.freePage(allocCount - 1);
        Partition part2 = new Partition(0);
        part2.open(emptyFile);
        int[] master = part2.getMasterPage();
        Assert.assertArrayEquals(master, new int[master.length]);
        byte[][] headers = part2.getHeaderPages();
        for (int i = 0; i < headers.length; i++){
            Assert.assertNull(headers[i]);
        }
    }

    @Test
    public void free_AllDataPages() throws Exception {
        Partition part1 = new Partition(0);
        part1.open(emptyFile);
        int allocCount = 10;
        for (int i = 0; i < allocCount; i++){
            part1.allocPage();
        }
        part1.freeDataPages();

        Partition part2 = new Partition(0);
        part2.open(emptyFile);
        int[] master = part2.getMasterPage();
        Assert.assertArrayEquals(master, new int[master.length]);
        byte[][] headers = part2.getHeaderPages();
        for (int i = 0; i < headers.length; i++){
            Assert.assertNull(headers[i]);
        }

    }

    private void comparePartition(Partition part1, Partition part2){
        Assert.assertArrayEquals(part1.getMasterPage(), part1.getMasterPage());
        byte[][] headerPages1 = part1.getHeaderPages();
        byte[][] headerPages2 = part2.getHeaderPages();
        Assert.assertEquals(headerPages1.length, headerPages2.length);
        for (int i = 0; i < headerPages1.length; i++){
            Assert.assertArrayEquals(headerPages1[i], headerPages2[i]);
        }
    }
}