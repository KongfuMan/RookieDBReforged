package org.csfundamental.database.query.expr;

import org.csfundamental.database.table.databox.DataBox;

public class AdditiveExpressionOperator extends ExpressionOperator{
    enum AdditiveExpressionOperatorType {
        Plus,
        Minus,
    }

    private AdditiveExpressionOperatorType type;

    public AdditiveExpressionOperator(String s) {
        if (s.equals("+")){
            this.type = AdditiveExpressionOperatorType.Plus;
        }else{
            this.type = AdditiveExpressionOperatorType.Minus;
        }
    }

    @Override
    public DataBox evaluate(DataBox left, DataBox right) {
        switch (type){
            case Plus -> DataBox.fromObject(left.getInt() + right.getInt());
            case Minus -> DataBox.fromObject(left.getInt() - right.getInt());
        }
        throw new RuntimeException("unknown type.");
    }
}
