package org.csfundamental.database.query.expr;

import org.csfundamental.database.table.Record;
import org.csfundamental.database.table.Schema;
import org.csfundamental.database.table.databox.DataBox;

/**
 * Column can be c
 * */
public class Column extends Expression {
    String name;

    Integer index;

    public Column(String column) {
        this.name = column;
        this.index = null;
    }

    @Override
    public DataBox evaluate(Record record) {
        this.index = this.schema.findField(this.name);
        return record.getValue(index);
    }
}
