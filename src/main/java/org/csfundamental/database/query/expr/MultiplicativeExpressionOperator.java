package org.csfundamental.database.query.expr;

import org.csfundamental.database.table.databox.DataBox;

public class MultiplicativeExpressionOperator extends ExpressionOperator {
    enum MultiplicativeOperatorType {
        Mul,
        Div,
        Mod,
    }

    private MultiplicativeOperatorType type;

    public MultiplicativeExpressionOperator(String op) {
        assert op != null;
        if(op.equals("*")){
            this.type = MultiplicativeOperatorType.Mul;
        }else if (op.equals("/")){
            this.type = MultiplicativeOperatorType.Div;
        }else {
            this.type = MultiplicativeOperatorType.Mod;
        }
    }

    @Override
    public DataBox evaluate(DataBox left, DataBox right) {
        switch (this.type){
            case Mul -> DataBox.fromObject(left.getInt() * right.getInt());
            case Div -> DataBox.fromObject(left.getInt() / right.getInt());
            case Mod -> DataBox.fromObject(left.getInt() % right.getInt());
        }
        throw new RuntimeException("unknown type.");
    }
}
