package org.csfundamental.database.table.databox;

public class ByteArrayDataBox extends DataBox{
    private byte[] values;
    private int size;

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
}
