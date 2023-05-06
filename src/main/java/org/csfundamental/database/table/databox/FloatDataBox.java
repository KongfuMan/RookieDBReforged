package org.csfundamental.database.table.databox;

import java.nio.ByteBuffer;

public class FloatDataBox extends DataBox{
    private float value;

    public FloatDataBox(float value){
        this.value = value;
    }

    @Override
    public Type type() {
        return Type.fromInt();
    }

    @Override
    public TypeId getTypeId() {
        return TypeId.FLOAT;
    }

    @Override
    public byte[] toBytes() {
        ByteBuffer buf = ByteBuffer.allocate(type().getSizeInBytes());
        buf.putFloat(value);
        return buf.array();
    }
}
