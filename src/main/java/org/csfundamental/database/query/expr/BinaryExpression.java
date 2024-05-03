package org.csfundamental.database.query.expr;

import org.csfundamental.database.table.Record;
import org.csfundamental.database.table.databox.DataBox;

/**
 * A decorator of ArithmeticExpression with left associative property.
 ** */
public abstract class BinaryExpression extends Expression {
    ExpressionOperator op;
    Expression expr;
    BinaryExpression left;

    public BinaryExpression(ExpressionOperator op, Expression expr, BinaryExpression binExpr){
        this.op = op;
        this.expr = expr;
        this.left = binExpr;
    }

    @Override
    public DataBox evaluate(Record record) {
        DataBox left = this.left.evaluate(record);
        DataBox right = this.expr.evaluate(record);
        return this.op.evaluate(left, right);
    }
}
