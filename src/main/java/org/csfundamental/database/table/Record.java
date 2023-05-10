package org.csfundamental.database.table;

import org.csfundamental.database.common.Buffer;
import org.csfundamental.database.table.databox.DataBox;
import org.csfundamental.database.table.databox.Type;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Records of the table.
 * **/
public class Record {
    List<DataBox> dataBoxes;

    public Record(List<DataBox> dataBoxes) {
        this.dataBoxes = dataBoxes;
    }

    public Record(Object... dataBoxes){
        this.dataBoxes = new ArrayList<>();
        for (Object dataBox: dataBoxes){
            this.dataBoxes.add((DataBox) dataBox);
        }
    }

    public static Record fromBytes(Buffer buf, Schema schema){
        List<DataBox> databoxes = new ArrayList<>();
        for (Type fieldType : schema.getFieldTypes()) {
            databoxes.add(DataBox.fromBytes(buf, fieldType));
        }
        return new Record(databoxes);
    }

    public byte[] toBytes(Schema schema){
        ByteBuffer buf = ByteBuffer.allocate(schema.getSizeInBytes());
        for (DataBox dataBox : dataBoxes){
            buf.put(dataBox.toBytes());
        }
        return buf.array();
    }

    public int size(){
        return dataBoxes.size();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null) return false;
        if (!(o instanceof Record)) return false;
        Record r = (Record) o;
        return dataBoxes.equals(r.dataBoxes);
    }

    @Override
    public int hashCode() {
        return dataBoxes.hashCode();
    }
}
