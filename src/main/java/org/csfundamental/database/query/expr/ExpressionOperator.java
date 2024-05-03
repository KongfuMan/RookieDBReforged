package org.csfundamental.database.query.expr;

import org.csfundamental.database.table.databox.DataBox;

public abstract class ExpressionOperator {
    public abstract DataBox evaluate(DataBox left, DataBox right);
}
