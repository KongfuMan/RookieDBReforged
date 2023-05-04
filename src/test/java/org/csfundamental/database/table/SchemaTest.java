package org.csfundamental.database.table;

import org.csfundamental.database.buffer.BufferManager;
import org.csfundamental.database.buffer.Page;
import org.csfundamental.database.common.Buffer;
import org.csfundamental.database.storage.DiskSpaceManager;
import org.csfundamental.database.storage.MemoryDiskSpaceManager;
import org.csfundamental.database.table.databox.Type;
import org.csfundamental.database.table.databox.TypeId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class SchemaTest {
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
        this.pageDirectory = new PageDirectory(bufferManager, partNum, firstHeaderPageNum);
    }

    @Test
    public void testCreateSchema(){
        Schema expectedSchema = new Schema();
        expectedSchema.add("id", Type.fromLong());
        expectedSchema.add("age", Type.fromInt());
        expectedSchema.add("first name", Type.fromString(256));
        expectedSchema.add("last name", Type.fromString(256));
        expectedSchema.add("weight", Type.fromFloat());

        short fullPageSize = this.pageDirectory.getEffectivePageSize();
        Page page = this.pageDirectory.fetchPageWithSpace(fullPageSize);
        Buffer pageBuffer = page.getBuffer().position(PageDirectory.DATA_HEADER_SIZE);
        pageBuffer.put(expectedSchema.toBytes());
        page.flush();

        PageDirectory newPageDir = new PageDirectory(bufferManager, partNum, firstHeaderPageNum);
        Page newPage = newPageDir.fetchPage(page.getPageNum());
        Buffer newBuf = newPage.getBuffer().position(PageDirectory.DATA_HEADER_SIZE);
        Schema actualSchema = Schema.fromBytes(newBuf);
        Assert.assertArrayEquals(actualSchema.toBytes(), expectedSchema.toBytes());
    }
}