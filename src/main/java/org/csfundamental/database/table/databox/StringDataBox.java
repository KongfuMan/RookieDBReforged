package org.csfundamental.database.table.databox;

import java.nio.ByteBuffer;

public class StringDataBox extends DataBox {
    private String value;
    private int size;

    public StringDataBox(String value, int size){
        if (size <= 0) {
            String msg = String.format("Cannot construct a %d-byte string. " +
                    "Strings must be at least one byte.", size);
            throw new IllegalArgumentException(msg);
        }
        if (value.length() > size){
            throw new IllegalArgumentException("length of input string exceeds the limit");
        }
        this.size = size;
        this.value = value.replaceAll("\0*$", ""); // Trim off null bytes
    }

    @Override
    public Type type() {
        return Type.fromString(size);
    }

    @Override
    public TypeId getTypeId() {
        return TypeId.STRING;
    }

    @Override
    public byte[] toBytes() {
        // padding
        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.put(value.getBytes());
        while(buf.hasRemaining()){
            buf.put((byte)0);
        }
        return buf.array();
    }
}
