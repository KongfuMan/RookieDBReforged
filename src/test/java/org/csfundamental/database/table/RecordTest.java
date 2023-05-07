package org.csfundamental.database.table;

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
        schema = new Schema();
        schema.add("id", Type.fromLong());
        schema.add("age", Type.fromInt());
        schema.add("name", Type.fromString(256));
        schema.add("weight", Type.fromFloat());
        schema.add("gender", Type.fromBool());
        schema.add("content", Type.fromByteArray(256));

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

        List<DataBox> dataBoxes = new ArrayList<>();
        dataBoxes.add(new LongDataBox(0L));
        dataBoxes.add(new IntDataBox(20));
        dataBoxes.add(new StringDataBox("Alice", 256));
        dataBoxes.add(new FloatDataBox(56.0F));
        dataBoxes.add(new BoolDataBox(true));
        dataBoxes.add(new ByteArrayDataBox(content, 256));
        Record record = new Record(dataBoxes);
        pageBuffer.put(record.toBytes(this.schema));
        page.flush();

        PageDirectory newPageDir = new PageDirectory(bufferManager, partNum, firstHeaderPageNum);
        Page newPage = newPageDir.fetchPage(page.getPageNum());
        Buffer newBuf = newPage.getBuffer().position(PageDirectory.DATA_HEADER_SIZE);
        Record actualRecord = Record.fromBytes(newBuf, schema);
        Assert.assertArrayEquals(record.toBytes(schema), actualRecord.toBytes(schema));
    }
}