package org.csfundamental.database.query.expr;

import org.csfundamental.database.table.databox.DataBox;

public class ComparisonExpressionOperator extends ExpressionOperator {
    enum ComparisonExpressionOperatorType {
        LT,
        LE,
        GT,
        GE,
        EQ,
        NEQ,
        ASSIGN
    }

    public ComparisonExpressionOperatorType type;
    public ComparisonExpressionOperator(String op) {
        if (op.equals("<")){
            this.type = ComparisonExpressionOperatorType.LT;
        }else if (op.equals("<=")){
            this.type = ComparisonExpressionOperatorType.LE;
        }else if (op.equals(">")){
            this.type = ComparisonExpressionOperatorType.GT;
        }else if (op.equals(">=")){
            this.type = ComparisonExpressionOperatorType.GE;
        }else if (op.equals("==")){
            this.type = ComparisonExpressionOperatorType.EQ;
        }else if (op.equals("!=")){
            this.type = ComparisonExpressionOperatorType.NEQ;
        }else if (op.equals("=")){
            this.type = ComparisonExpressionOperatorType.ASSIGN;
        } else {
            throw new RuntimeException(String.format("Unknown comparison operator: %s", op));
        }
    }

    @Override
    public DataBox evaluate(DataBox left, DataBox right) {
        switch (this.type){
            case LT -> DataBox.fromObject(left.getInt() < right.getInt());
            case LE -> DataBox.fromObject(left.getInt() <= right.getInt());
            case GT -> DataBox.fromObject(left.getInt() > right.getInt());
            case GE -> DataBox.fromObject(left.getInt() >= right.getInt());
            case EQ -> DataBox.fromObject(left.getInt() == right.getInt());
            case NEQ -> DataBox.fromObject(left.getInt() != right.getInt());
        }
        throw new RuntimeException("Unknown type.");
    }
}
