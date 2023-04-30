package org.csfundamental.database.table;

import org.csfundamental.database.buffer.BufferManager;
import org.csfundamental.database.buffer.Page;
import org.csfundamental.database.storage.DiskSpaceManager;
import org.csfundamental.database.storage.MemoryDiskSpaceManager;
import org.csfundamental.database.storage.PageException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

public class PageDirectoryTest {
    private DiskSpaceManager diskSpaceManager;
    private BufferManager bufferManager;
    private PageDirectory pageDirectory;
    private int partNum;
    private long firstHeaderPageNum;

    @Before
    public void beforeEach() {
        this.diskSpaceManager = new MemoryDiskSpaceManager();
        this.partNum = this.diskSpaceManager.allocPart();
        this.firstHeaderPageNum = this.diskSpaceManager.allocPage(this.partNum);
        this.bufferManager = new BufferManager(this.diskSpaceManager, 1024);
    }

    @Test
    public void testCreatePageDirectory(){
        this.pageDirectory = new PageDirectory(bufferManager, partNum, firstHeaderPageNum);
    }

    @Test
    public void testFetchPageWithSpace(){
        this.pageDirectory = new PageDirectory(bufferManager, partNum, firstHeaderPageNum);
        short fullPageSize = pageDirectory.getEffectivePageSize();
        Page p1 = pageDirectory.fetchPageWithSpace(fullPageSize);
        Page p2 = pageDirectory.fetchPageWithSpace((short) 1);
        Page p3 = pageDirectory.fetchPageWithSpace((short) 60);
        Page p4 = pageDirectory.fetchPageWithSpace((short) (fullPageSize - 60));
        Page p5 = pageDirectory.fetchPageWithSpace((short) 120);

        p1.unpin(); p2.unpin(); p3.unpin(); p4.unpin(); p5.unpin();
        assertEquals(1, p1.getPageNum());
        assertEquals(2, p2.getPageNum());
        assertEquals(2, p3.getPageNum());
        assertEquals(3, p4.getPageNum());
        assertEquals(2, p5.getPageNum());

        assertNotEquals(p1, p2);
        assertEquals(p2, p3);
        assertEquals(p3, p5);
        assertNotEquals(p4, p5);
    }

    @Test
    public void testFetchPageWithSpaceFromTwoHeaders(){
        this.pageDirectory = new PageDirectory(bufferManager,partNum, firstHeaderPageNum);
        short fullPageSize = pageDirectory.getEffectivePageSize();

        int expectedPageNum = 1;
        for (int i = 0; i < PageDirectory.HEADER_ENTRY_COUNT; i++){
            Page page = pageDirectory.fetchPageWithSpace(fullPageSize);
            assertEquals(expectedPageNum++, page.getPageNum());
        }

        for (int i = 0; i < PageDirectory.HEADER_ENTRY_COUNT; i++){
            Page page = pageDirectory.fetchPageWithSpace(fullPageSize);
            assertEquals(++expectedPageNum, page.getPageNum());
        }
    }

    @Test
    public void testGetPageWithSpaceInvalid() {
        this.pageDirectory = new PageDirectory(bufferManager,partNum, firstHeaderPageNum);
        short fullPageSize = pageDirectory.getEffectivePageSize();
        short[] invalidSizes = new short[]{(short)(fullPageSize + 1), 0, -1};
        for (short invalidSize : invalidSizes){
            Assert.assertThrows(IllegalArgumentException.class, new ThrowingRunnable(){
                @Override
                public void run() {
                    pageDirectory.fetchPageWithSpace(invalidSize);
                }
            });
        }
    }

    @Test
    public void testFetchPage() {
        this.pageDirectory = new PageDirectory(bufferManager,partNum, firstHeaderPageNum);
        short fullPageSize = pageDirectory.getEffectivePageSize();
        Page p1 = pageDirectory.fetchPageWithSpace(fullPageSize);
        Page p2 = pageDirectory.fetchPageWithSpace((short) 1);
        Page p3 = pageDirectory.fetchPageWithSpace((short) 60);
        Page p4 = pageDirectory.fetchPageWithSpace((short) (fullPageSize - 60));
        Page p5 = pageDirectory.fetchPageWithSpace((short) 120);

        p1.unpin(); p2.unpin(); p3.unpin(); p4.unpin(); p5.unpin();

        Page pp1 = pageDirectory.fetchPage(p1.getPageNum());
        Page pp2 = pageDirectory.fetchPage(p2.getPageNum());
        Page pp3 = pageDirectory.fetchPage(p3.getPageNum());
        Page pp4 = pageDirectory.fetchPage(p4.getPageNum());
        Page pp5 = pageDirectory.fetchPage(p5.getPageNum());

        pp1.unpin(); pp2.unpin(); pp3.unpin(); pp4.unpin(); pp5.unpin();

        assertEquals(p1, pp1);
        assertEquals(p2, pp2);
        assertEquals(p3, pp3);
        assertEquals(p4, pp4);
        assertEquals(p5, pp5);
    }

    @Test
    public void testUpdateFreeSpace() {
        this.pageDirectory = new PageDirectory(bufferManager,partNum, firstHeaderPageNum);
        short pageSize = (short) (pageDirectory.getEffectivePageSize() - 10);
        Page p1 = pageDirectory.fetchPageWithSpace(pageSize);
        p1.unpin();

        pageDirectory.updateFreeSpace(p1, (short) 10);
        Page p2 = pageDirectory.fetchPageWithSpace((short) 10);

        p2.unpin();
        assertEquals(p1, p2);

        pageDirectory.updateFreeSpace(p1, (short) 10);
        Page p3 = pageDirectory.fetchPageWithSpace((short) 20);

        p3.unpin();
        assertNotEquals(p1, p3);
    }

    @Test
    public void testUpdateFreeSpaceOfPageSize() {
        this.pageDirectory = new PageDirectory(bufferManager,partNum, firstHeaderPageNum);

        short fullPageSize = pageDirectory.getEffectivePageSize();
        Page dataPage = pageDirectory.fetchPageWithSpace(fullPageSize);
        dataPage.unpin();

        pageDirectory.updateFreeSpace(dataPage, fullPageSize);
        try {
            pageDirectory.updateFreeSpace(dataPage, (short) 10);
            fail();
        } catch (PageException e) { /* do nothing */ }
    }

    @Test
    public void testIterator() {
        this.pageDirectory = new PageDirectory(bufferManager,partNum, firstHeaderPageNum);
        short fullPageSize = this.pageDirectory.getEffectivePageSize();

        int numRequests = 100;
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < numRequests; ++i) {
            Page page = pageDirectory.fetchPageWithSpace((short)(fullPageSize / 2));
            if (pages.size() == 0 || !pages.get(pages.size() - 1).equals(page)) {
                pages.add(page);
            }
            page.unpin();
        }

        Iterator<Page> iter = pageDirectory.iterator();
        for (Page page : pages) {
            assertTrue(iter.hasNext());

            Page p = iter.next();
            p.unpin();
            assertEquals(page, p);
        }
    }

    @Test
    public void testIteratorWithDeletes() {
        this.pageDirectory = new PageDirectory(bufferManager,partNum, firstHeaderPageNum);
        short fullPageSize = this.pageDirectory.getEffectivePageSize();

        int numRequests = 100;
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < numRequests; ++i) {
            Page page = pageDirectory.fetchPageWithSpace((short)(fullPageSize / 2));
            if (pages.size() == 0 || !pages.get(pages.size() - 1).equals(page)) {
                pages.add(page);
            }
            page.unpin();
        }

        Iterator<Page> iterator = pages.iterator();
        while (iterator.hasNext()) {
            // free one page and the page after next page, and so on.
            iterator.next();
            if (iterator.hasNext()) {
                pageDirectory.updateFreeSpace(iterator.next(), fullPageSize);
                iterator.remove();
            }
        }

        Iterator<Page> iter = pageDirectory.iterator();
        for (Page page : pages) {
            assertTrue(iter.hasNext());

            Page p = iter.next();
            p.unpin();
            assertEquals(page, p);
        }
    }
}