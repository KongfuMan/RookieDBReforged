package org.csfundamental.database.query.expr;

import org.csfundamental.database.table.Record;
import org.csfundamental.database.table.databox.DataBox;

/** String | FLOAT | LONG | INT | TRUE | FALSE
 * */
public class Literal extends Expression{
    DataBox data;

    public Literal(DataBox data) {
        this.data = data;
    }

    @Override
    public DataBox evaluate(Record record) {
        return data;
    }
}
