package org.csfundamental.database.table;

import org.csfundamental.database.common.Buffer;
import org.csfundamental.database.table.databox.Type;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


public class Schema {
    public static int MAX_SCHEMA_SIZE;
    private List<String> fieldNames;
    private List<Type> fieldTypes;
    private int sizeInBytes;

    public Schema() {
        this.fieldNames = new ArrayList<>();
        this.fieldTypes = new ArrayList<>();
        this.sizeInBytes = 0;
    }

    public Schema(List<String> fieldNames, List<Type> fieldTypes){
        assert fieldNames.size() == fieldTypes.size();
        assert fieldNames.size() > 0;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    public Schema add(String fieldName, Type fieldType){
        this.fieldNames.add(fieldName);
        this.fieldTypes.add(fieldType);
        this.sizeInBytes +=  fieldType.getSizeInBytes();
        return this;
    }

    public static Schema fromBytes(Buffer buf){
        Schema schema = new Schema();
        int fieldCount = buf.getInt();
        for (int i = 0; i < fieldCount; i++){
            int fieldNameLen = buf.getInt();
            byte[] chars = new byte[fieldNameLen];
            buf.get(chars);
            schema.add(new String(chars), Type.fromBytes(buf));
        }
        return schema;
    }

    // A schema is serialized as follows.
    // We first write the number of fields(4 bytes).
    // Then, for each field, we write:
    //   1. the length of the field name (4 bytes),
    //   2. the field's name as String
    //   3. and the field's type(fixed size: 8 ybtes).
    public byte[] toBytes(){
        int size = Integer.BYTES; // number of fields
        for (int i = 0; i < fieldNames.size(); i++){
            size += Integer.BYTES; // size in bytes of each field name.
            size += fieldNames.get(i).length();
            size += fieldTypes.get(i).getSizeInBytes();
        }

        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.putInt(fieldNames.size());
        for (int i = 0; i < fieldNames.size(); i++)
        {
            buf.putInt(fieldNames.get(i).length());
            buf.put(fieldNames.get(i).getBytes());
            Type type = fieldTypes.get(i);
            byte[] typeBytes = type.toBytes();
            buf.put(typeBytes);
        }
        return buf.array();
    }

    public int getSizeInBytes(){
        return sizeInBytes;
    }

    public List<Type> getFieldTypes(){
        return fieldTypes;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null) return false;
        if (!(o instanceof Schema)) return false;
        Schema s = (Schema) o;
        return fieldNames.equals(s.fieldNames) && fieldTypes.equals(s.fieldTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldNames, fieldTypes);
    }
}
