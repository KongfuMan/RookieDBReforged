package org.csfundamental.database.query.expr;

import org.csfundamental.database.table.Record;
import org.csfundamental.database.table.Schema;
import org.csfundamental.database.table.databox.DataBox;

public abstract class Expression {

    protected Schema schema;

    public abstract DataBox evaluate(Record record);
}
