package org.csfundamental.database.query.expr;

import org.csfundamental.database.table.Record;
import org.csfundamental.database.table.databox.DataBox;

public class AdditiveExpression extends BinaryExpression {
    public AdditiveExpression(ExpressionOperator op, Expression expr, BinaryExpression binExpr){
        super(op, expr, binExpr);
    }
}
