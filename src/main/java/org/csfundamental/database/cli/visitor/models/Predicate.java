package org.csfundamental.database.cli.visitor.models;

import org.csfundamental.database.query.expr.ComparisonExpressionOperator;
import org.csfundamental.database.table.databox.DataBox;

public class Predicate {
    String column;

    ComparisonExpressionOperator op;

    DataBox dataBox;

    public Predicate(String column, ComparisonExpressionOperator op, DataBox dataBox) {
        this.column = column;
        this.op = op;
        this.dataBox = dataBox;
    }
}
