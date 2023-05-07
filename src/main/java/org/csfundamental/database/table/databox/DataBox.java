package org.csfundamental.database.table.databox;

import org.csfundamental.database.common.Buffer;

public abstract class DataBox {
    public abstract Type type();
    public abstract TypeId getTypeId();

    public boolean getBool() { throw new RuntimeException("not boolean type"); }

    public int getInt() { throw new RuntimeException("not int type"); }

    public float getFloat() { throw new RuntimeException("not float type"); }

    public String getString() { throw new RuntimeException("not String type"); }

    public long getLong() {
        throw new RuntimeException("not Long type");
    }

    public byte[] getByteArray() { throw new RuntimeException("not Byte Array type"); }

    public static DataBox fromBytes(Buffer buf, Type type){
        switch (type.typeId){
            case BOOL -> {
                byte b = buf.get();
                assert (b == 0 || b == 1);
                return new BoolDataBox(b == 1);
            }
            case INT -> {
                return new IntDataBox(buf.getInt());
            }
            case FLOAT -> {
                return new FloatDataBox(buf.getFloat());
            }
            case LONG -> {
                return new LongDataBox(buf.getLong());
            }
            case  STRING -> {
                int size = type.getSizeInBytes();
                byte[] bytes = new byte[size];
                buf.get(bytes);
                return new StringDataBox(new String(bytes), size);
            }
            case BYTE_ARRAY -> {
                int size = type.getSizeInBytes();
                byte[] bytes = new byte[size];
                buf.get(bytes);
                return new ByteArrayDataBox(bytes, size);
            }
            default -> {
                String err = String.format("Unhandled TypeId %s.", type.getTypeId().toString());
                throw new IllegalArgumentException(err);
            }
        }
    }

    public abstract byte[] toBytes();
}
