package org.csfundamental.database.table.databox;

import java.util.Arrays;

public class ByteArrayDataBox extends DataBox{
    private final byte[] values;
    private final int size;

    public ByteArrayDataBox(byte[] values, int size){
        if (values.length != size){
            throw  new IllegalArgumentException("");
        }
        this.values = values;
        this.size = size;
    }

    @Override
    public Type type() {
        return Type.fromByteArray(this.size);
    }

    @Override
    public TypeId getTypeId() {
        return TypeId.BYTE_ARRAY;
    }

    @Override
    public byte[] toBytes() {
        return values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ByteArrayDataBox other)) return false;
        return Arrays.equals(this.values, other.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }
}
