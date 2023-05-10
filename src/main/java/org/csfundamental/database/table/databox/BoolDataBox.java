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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BoolDataBox)) {
            return false;
        }
        BoolDataBox b = (BoolDataBox) o;
        return this.value == b.value;
    }

    @Override
    public int hashCode() {
        return Boolean.valueOf(value).hashCode();
    }
}
