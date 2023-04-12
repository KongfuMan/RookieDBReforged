package org.csfundamental.database.table;

import org.csfundamental.database.buffer.BufferFrame;
import org.csfundamental.database.buffer.Page;

/**
 * Data pages contain a small header containing:
 * - 4-byte page directory id
 * - 4-byte index of which header page manages it
 * - 2-byte offset indicating which slot in the header page its data page entry resides
 */
public class DataPage extends Page {

    public DataPage(BufferFrame frame) {
        super(frame);
    }
}
