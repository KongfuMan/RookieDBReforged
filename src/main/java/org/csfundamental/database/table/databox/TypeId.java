package org.csfundamental.database.table.databox;

public enum TypeId {
    BOOL,
    INT,
    FLOAT,
    LONG,
    STRING,
    BYTE_ARRAY;

    private static final TypeId[] values = TypeId.values();

    public static TypeId fromInt(int typeId){
        if (typeId < 0 || typeId > 5){
            throw new IllegalArgumentException("expected value should be [0, 5]");
        }
        return values[typeId];
    }
}
