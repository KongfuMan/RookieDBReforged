package org.csfundamental.database.query;

import org.csfundamental.database.table.Schema;

public abstract class QueryOperator implements Iterable<Record> {
    public Schema getSchema() {
        return null;
    }
}
