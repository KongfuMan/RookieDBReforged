package org.csfundamental.database.table;

import org.csfundamental.database.TestUtils;
import org.csfundamental.database.buffer.BufferManager;
import org.csfundamental.database.buffer.Page;
import org.csfundamental.database.common.Buffer;
import org.csfundamental.database.storage.DiskSpaceManager;
import org.csfundamental.database.storage.MemoryDiskSpaceManager;
import org.csfundamental.database.table.databox.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RecordTest {
    private DiskSpaceManager diskSpaceManager;
    private BufferManager bufferManager;
    private PageDirectory pageDirectory;
    private int partNum;
    private long firstHeaderPageNum;
    private Schema schema;

    @Before
    public void beforeEach() {
        this.diskSpaceManager = new MemoryDiskSpaceManager();
        this.partNum = this.diskSpaceManager.allocPart();
        this.firstHeaderPageNum = this.diskSpaceManager.allocPage(this.partNum);
        this.bufferManager = new BufferManager(this.diskSpaceManager, 1024);
        this.pageDirectory = new PageDirectory(bufferManager, partNum, firstHeaderPageNum);

        //create the schema
        schema = TestUtils.createSchemaWithAllTypes();

        short fullPageSize = this.pageDirectory.getEffectivePageSize();
        Page page = this.pageDirectory.fetchPageWithSpace(fullPageSize);
        Buffer pageBuffer = page.getBuffer().position(PageDirectory.DATA_HEADER_SIZE);
        pageBuffer.put(schema.toBytes());
        page.flush();
    }

    @Test
    public void testCreateSchema(){
        short fullPageSize = this.pageDirectory.getEffectivePageSize();
        Page page = this.pageDirectory.fetchPageWithSpace(fullPageSize);
        Buffer pageBuffer = page.getBuffer().position(PageDirectory.DATA_HEADER_SIZE);

        byte[] content = new byte[256];
        Random rand = new Random();
        rand.nextBytes(content);

        Record record = TestUtils.createRecordWithAllTypes();
        pageBuffer.put(record.toBytes(this.schema));
        page.flush();

        PageDirectory newPageDir = new PageDirectory(bufferManager, partNum, firstHeaderPageNum);
        Page newPage = newPageDir.fetchPage(page.getPageNum());
        Buffer newBuf = newPage.getBuffer().position(PageDirectory.DATA_HEADER_SIZE);
        Record actualRecord = Record.fromBytes(newBuf, schema);
        Assert.assertEquals(actualRecord, record);
    }
}