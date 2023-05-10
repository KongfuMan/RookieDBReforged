package org.csfundamental.database.table.databox;

import java.nio.ByteBuffer;

public class IntDataBox extends DataBox{
    private int value;

    public IntDataBox(int value){
        this.value = value;
    }

    @Override
    public Type type() {
        return Type.fromInt();
    }

    @Override
    public TypeId getTypeId() {
        return TypeId.INT;
    }

    @Override
    public int getInt() {
        return value;
    }

    @Override
    public byte[] toBytes() {
        int size = type().getSizeInBytes();
        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.putInt(getInt());
        return buf.array();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof IntDataBox)) {
            return false;
        }
        IntDataBox i = (IntDataBox) o;
        return this.value == i.value;
    }

    @Override
    public int hashCode() {
        return Integer.valueOf(value).hashCode();
    }
}
