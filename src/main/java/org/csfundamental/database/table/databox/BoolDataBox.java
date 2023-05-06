package org.csfundamental.database.table.databox;

import java.nio.ByteBuffer;

public class BoolDataBox extends DataBox{
    private boolean value;

    public BoolDataBox(boolean value){
        this.value = value;
    }

    @Override
    public Type type() {
        return Type.fromBool();
    }

    @Override
    public TypeId getTypeId() {
        return TypeId.BOOL;
    }

    @Override
    public byte[] toBytes() {
        ByteBuffer buf = ByteBuffer.allocate(type().getSizeInBytes());
        buf.put((byte)(value ? 1 : 0));
        return buf.array();
    }
}
