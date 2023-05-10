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

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof LongDataBox)) {
            return false;
        }
        LongDataBox l = (LongDataBox) o;
        return this.value == l.value;
    }

    @Override
    public int hashCode() {
        return Long.valueOf(value).hashCode();
    }
}
