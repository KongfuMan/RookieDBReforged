package org.csfundamental.database;


import org.csfundamental.database.table.Record;
import org.csfundamental.database.table.Schema;
import org.csfundamental.database.table.databox.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TestUtils {
    public static Schema createSchemaWithAllTypes() {
        return new Schema()
                .add("id", Type.fromLong())
                .add("age", Type.fromInt())
                .add("first name", Type.fromString(256))
                .add("last name", Type.fromString(256))
                .add("weight", Type.fromFloat())
                .add("gender", Type.fromBool())
                .add("content", Type.fromByteArray(256));
    }

    public static Record createRecordWithAllTypes() {
        return createRecordWithAllTypes(20);
    }

    public static Record createRecordWithAllTypes(int var) {
        byte[] content = new byte[256];
        Random rand = new Random();
        rand.nextBytes(content);

        List<DataBox> dataBoxes = new ArrayList<>();
        dataBoxes.add(new LongDataBox(0L));
        dataBoxes.add(new IntDataBox(var));
        dataBoxes.add(new StringDataBox("Alice", 256));
        dataBoxes.add(new StringDataBox("King", 256));
        dataBoxes.add(new FloatDataBox(56.0F));
        dataBoxes.add(new BoolDataBox(true));
        dataBoxes.add(new ByteArrayDataBox(content, 256));
        return new Record(dataBoxes);
    }

//    public static TestSourceOperator createSourceWithAllTypes(int numRecords) {
//        Schema schema = createSchemaWithAllTypes();
//        List<Record> records = new ArrayList<>();
//        for (int i = 0; i < numRecords; i++)
//            records.add(createRecordWithAllTypes());
//        return new TestSourceOperator(records, schema);
//    }
//
//    public static Record createRecordWithAllTypesWithValue(int val) {
//        return new Record(true, val, "" + (char) (val % 79 + 0x30), 1.0f);
//    }
//
//    public static TestSourceOperator createSourceWithInts(List<Integer> values) {
//        Schema schema = new Schema().add("int", Type.intType());
//        List<Record> recordList = new ArrayList<Record>();
//        for (int v : values) recordList.add(new Record(v));
//        return new TestSourceOperator(recordList, schema);
//    }
}
