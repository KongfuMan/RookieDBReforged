package org.csfundamental.database.query.expr;

import org.csfundamental.database.table.Record;
import org.csfundamental.database.table.databox.DataBox;

public class PrimaryExpression extends Expression {
    // literal, func_call, column_name, expr, add_op + primary_expr
    Expression childExpr;

    ExpressionOperator addOp;

    public void setChildExpr(Expression childExpr){
        this.childExpr = childExpr;
    }

    public void setAdditiveOperator(ExpressionOperator operator){
        this.addOp = operator;
    }

    @Override
    public DataBox evaluate(Record record) {
        DataBox databox = childExpr.evaluate(record);
        return addOp.evaluate(null, databox);
    }
}
