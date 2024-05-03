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

    public static DataBox fromString(Type type, String s) {
        String raw = s;
        s = s.toLowerCase().trim();
        switch (type.getTypeId()) {
            case BOOL: return new BoolDataBox(s.equals("true"));
            case INT: return new IntDataBox(Integer.parseInt(s));
            case LONG: return new LongDataBox(Long.parseLong(s));
            case FLOAT: return new FloatDataBox(Float.parseFloat(s));
            case STRING: return new StringDataBox(raw);
            default: throw new RuntimeException("Unreachable code");
        }
    }

    /**
     * @param o some object
     * @return if the passed in object was already DataBox then we return the
     * object after a cast. Otherwise, if the object was an instance of a
     * wrapper type for one of the primitives we support, then return a DataBox
     * of the proper type wrapping the object. Useful for making the record
     * constructor and QueryPlan methods more readable.
     *
     * Examples:
     * - DataBox.fromObject(186) ==  new IntDataBox(186)
     * - DataBox.fromObject("186") == new StringDataBox("186")
     * - DataBox.fromObject(new ArrayList<>()) // Error! ArrayList isn't a
     *                                         // primitive we support
     */
    public static DataBox fromObject(Object o) {
        if (o instanceof DataBox) return (DataBox) o;
        if (o instanceof Integer) return new IntDataBox((Integer) o);
        if (o instanceof String) return new StringDataBox((String) o);
        if (o instanceof Boolean) return new BoolDataBox((Boolean) o);
        if (o instanceof Long) return new LongDataBox((Long) o);
        if (o instanceof Float) return new FloatDataBox((Float) o);
        if (o instanceof Double) {
            // implicit cast
            double d = (Double) o;
            return new FloatDataBox((float) d);
        }
        if (o instanceof byte[]) return new ByteArrayDataBox((byte[]) o, ((byte[]) o).length);
        throw new IllegalArgumentException("Object was not a supported data type");
    }
}
