package org.csfundamental.database.table.databox;

import org.csfundamental.database.common.Buffer;

import java.nio.ByteBuffer;

public class Type{
    TypeId typeId;
    int sizeInBytes;


    public static Type fromBool(){
        return new Type(TypeId.BOOL, Byte.BYTES);
    }

    public static Type fromInt(){
        return new Type(TypeId.INT, Integer.BYTES);
    }

    public static Type fromFloat(){
        return new Type(TypeId.FLOAT, Float.BYTES);
    }

    public static Type fromLong(){
        return new Type(TypeId.LONG, Long.BYTES);
    }

    public static Type fromString(int len){
        return new Type(TypeId.STRING, len);
    }

    public static Type fromByteArray(int len){
        return new Type(TypeId.BYTE_ARRAY, len);
    }

    private Type(int typeId, int sizeInBytes){
        this(TypeId.fromInt(typeId), sizeInBytes);
    }

    private Type(TypeId typeId, int sizeInBytes){
        checkTypeSize(typeId, sizeInBytes);
        this.typeId = typeId;
        this.sizeInBytes = sizeInBytes;
    }

    private void checkTypeSize(TypeId typeId, int typeSize){
        switch (typeId) {
            case BOOL -> {
                assert typeSize == Byte.BYTES;
            }
            case INT -> {
                assert typeSize == Integer.BYTES;
            }
            case FLOAT -> {
                assert typeSize == Float.BYTES;
            }
            case LONG -> {
                assert typeSize == Long.BYTES;
            }
            default -> {
                assert typeSize > 0 && typeSize <= 256;
            }
        }
    }

    public static Type fromBytes(Buffer buf){
        TypeId typeId = TypeId.fromInt(buf.getInt());
        int typeSize = buf.getInt();
        return new Type(typeId, typeSize);
    }

    // serialize as follows:
    // 4 bytes: enum type id
    // 4 bytes: type size;
    public byte[] toBytes(){
        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES * 2);
        buf.putInt(typeId.ordinal());
        buf.putInt(sizeInBytes);
        return buf.array();
    }

    public TypeId getTypeId(){
        return typeId;
    }

    public int getSizeInBytes(){
        return sizeInBytes;
    }
}
