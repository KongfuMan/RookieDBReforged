package org.csfundamental.database.table.databox;

import java.nio.ByteBuffer;

public class LongDataBox extends DataBox{
    private long value;

    public LongDataBox(long value){
        this.value = value;
    }

    @Override
    public Type type() {
        return Type.fromLong();
    }

    @Override
    public TypeId getTypeId() {
        return TypeId.LONG;
    }

    @Override
    public byte[] toBytes() {
        ByteBuffer buf = ByteBuffer.allocate(type().getSizeInBytes());
        buf.putLong(value);
        return buf.array();
    }
}
